from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel
import secrets
import os
from typing import Dict, List

app = FastAPI()

# Metadatos en memoria:
# files[filename] = {
#   "size": int,
#   "block_size": int,
#   "blocks": [{"index": int, "block_id": str, "datanode": str}]
# }
files = {}
users = {}
directories = {}

# Lista de DataNodes configurable por env (coma-separada)
# ej: DATANODES="http://datanode1:5000,http://datanode2:5000"
DATANODES = [d.strip() for d in os.getenv("DATANODES", "").split(",") if d.strip()]
if not DATANODES:
    # fallback útil en compose con servicios datanode1/datanode2
    DATANODES = ["http://datanode1:5000", "http://datanode2:5000"]


def get_current_user(token: str = Header(...)):
    for username, data in users.items():
        if data.get("token") == token:
            return username
    raise HTTPException(status_code=401, detail="Token inválido")
    
@app.get("/")
def root():
    return {"status": "namenode alive", "datanodes": DATANODES}

@app.get("/datanodes")
def get_datanodes(username: str = Depends(get_current_user)):
    return {"datanodes": DATANODES}

class BlockInfo(BaseModel):
    index: int
    block_id: str
    datanode: str

class FileRegistration(BaseModel):
    filename: str
    size: int
    block_size: int
    blocks: list[BlockInfo]

class UserRegistration(BaseModel):
    username: str
    password: str

def get_current_user(token: str = Header(...)):
    for username, data in users.items():
        if data.get("token") == token:
            return username
    raise HTTPException(status_code=401, detail="Token inválido")

@app.post("/register_file")
def register_file(reg: FileRegistration, username: str = Depends(get_current_user)):
    normalized_filename = f"/{reg.filename}" if not reg.filename.startswith("/") else reg.filename
    
    # CREATE PARENT DIRECTORIES AUTOMATICALLY - THIS IS THE KEY FIX!
    parent_dir = os.path.dirname(normalized_filename)
    if parent_dir != "/":  # Don't create root directory
        dir_key = (username, parent_dir)
        if dir_key not in directories:
            directories[dir_key] = {"type": "directory", "owner": username}
            print(f"Auto-created directory: {parent_dir}")
    
    files[(username, normalized_filename)] = {
        "size": reg.size,
        "block_size": reg.block_size,
        "blocks": sorted([b.dict() for b in reg.blocks], key=lambda x: x["index"])
    }
    print(f"Registered file: {normalized_filename}")
    return {"msg": "Archivo registrado", "filename": normalized_filename, "owner": username}


@app.get("/file/{filename:path}")  
def get_file(filename: str, username: str = Depends(get_current_user)):  
    if not filename.startswith("/"):
        normalized_filename = f"/{filename}"
    else:
        normalized_filename = filename
 
    normalized_filename = normalized_filename.replace("//", "/").rstrip("/")
    key = (username, normalized_filename)
    
    print(f"🔍 DEBUG GET_FILE:")
    print(f"   username: '{username}'")
    print(f"   filename: '{filename}'")
    print(f"   normalized_filename: '{normalized_filename}'")
    print(f"   key: {key}")
    print(f"   ALL files keys: {list(files.keys())}")
    print(f"   User files keys: {[k for k in files.keys() if k[0] == username]}")
    
    if key not in files:
        # Try alternative path formats for better compatibility
        alt_key1 = (username, normalized_filename + "/")
        alt_key2 = (username, normalized_filename.lstrip("/"))
        alt_key3 = (username, "/" + normalized_filename.lstrip("/"))
        
        if alt_key1 in files:
            return files[alt_key1]
        elif alt_key2 in files:
            return files[alt_key2]
        elif alt_key3 in files:
            return files[alt_key3]
        else:
            raise HTTPException(status_code=404, detail="File not found")
    
    return files[key]


@app.delete("/rm/{filename:path}")
def rm(filename: str, username: str = Depends(get_current_user)):
    normalized_filename = f"/{filename}" if not filename.startswith("/") else filename
    key = (username, normalized_filename)
    
    if key not in files:
        raise HTTPException(status_code=404, detail="File not found")
    
    del files[key]
    return {"msg": f"File removed: {normalized_filename}"}


@app.post("/register")
def register(user: UserRegistration):
    if user.username in users:
        raise HTTPException(status_code=400, detail="Usuario ya existe")
    users[user.username] = {"password": user.password, "token": None}
    return {"msg": "Usuario registrado"}

@app.post("/login")
def login(user: UserRegistration):
    if user.username not in users or users[user.username]["password"] != user.password:
        raise HTTPException(status_code=401, detail="Credenciales inválidas")
    token = secrets.token_hex(16)
    users[user.username]["token"] = token
    return {"token": token}

@app.post("/mkdir/{path}")
def mkdir(path: str, username: str = Depends(get_current_user)):
    """Create a directory"""
    dir_path = f"/{path}" if not path.startswith("/") else path
    key = (username, dir_path)
    
    if key in files or key in directories:
        raise HTTPException(status_code=400, detail="Path already exists")
    
    directories[key] = {"type": "directory", "owner": username}
    return {"msg": f"Directory created: {dir_path}"}

@app.delete("/rmdir/{path}")
def rmdir(path: str, username: str = Depends(get_current_user)):
    """Remove a directory"""
    dir_path = f"/{path}" if not path.startswith("/") else path
    key = (username, dir_path)
    
    if key not in directories:
        raise HTTPException(status_code=404, detail="Directory not found")
    
    # Check if directory is empty
    dir_prefix = dir_path + "/"
    for (owner, file_path) in list(files.keys()) + list(directories.keys()):
        if owner == username and file_path.startswith(dir_prefix):
            raise HTTPException(status_code=400, detail="Directory not empty")
    
    del directories[key]
    return {"msg": f"Directory removed: {dir_path}"}

@app.get("/ls")
def ls(path: str = "/", username: str = Depends(get_current_user)):
    """List files and directories"""
    # Normalize the path - ensure it starts with /
    if not path.startswith("/"):
        path = "/" + path
    
    if path != "/" and (username, path) not in directories:
        raise HTTPException(status_code=404, detail="Path not found")
    
    items = []
    
    # Add directories
    for (owner, dir_path), meta in directories.items():
        if owner == username and dir_path.startswith(path) and dir_path != path:
            relative_path = dir_path[len(path):].lstrip("/")
            if "/" not in relative_path:  # Only immediate children
                items.append({
                    "name": relative_path,
                    "type": "directory",
                    "size": 0
                })
    
    # Add files
    for (owner, file_path), meta in files.items():
        if owner == username and file_path.startswith(path):
            relative_path = file_path[len(path):].lstrip("/")
            if "/" not in relative_path:  # Only immediate children
                items.append({
                    "name": relative_path,
                    "type": "file",
                    "size": meta["size"],
                    "block_size": meta["block_size"]
                })
    
    return {"path": path, "items": items}