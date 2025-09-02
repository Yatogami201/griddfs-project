from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os

app = FastAPI()

# Metadatos en memoria:
# files[filename] = {
#   "size": int,
#   "block_size": int,
#   "blocks": [{"index": int, "block_id": str, "datanode": str}]
# }
files = {}

# Lista de DataNodes configurable por env (coma-separada)
# ej: DATANODES="http://datanode1:5000,http://datanode2:5000"
DATANODES = [d.strip() for d in os.getenv("DATANODES", "").split(",") if d.strip()]
if not DATANODES:
    # fallback útil en compose con servicios datanode1/datanode2
    DATANODES = ["http://datanode1:5000", "http://datanode2:5000"]

@app.get("/")
def root():
    return {"status": "namenode alive", "datanodes": DATANODES}

@app.get("/datanodes")
def get_datanodes():
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

@app.post("/register_file")
def register_file(reg: FileRegistration):
    files[reg.filename] = {
        "size": reg.size,
        "block_size": reg.block_size,
        "blocks": sorted([b.dict() for b in reg.blocks], key=lambda x: x["index"])
    }
    return {"msg": "registered", "filename": reg.filename}

@app.get("/file/{filename}")
def get_file(filename: str):
    if filename not in files:
        raise HTTPException(status_code=404, detail="File not found")
    return files[filename]

@app.get("/ls")
def ls():
    return {"files": [{"filename": k, **v} for k, v in files.items()]}

@app.delete("/rm/{filename}")
def rm(filename: str):
    if filename not in files:
        raise HTTPException(status_code=404, detail="File not found")
    del files[filename]
    return {"msg": "removed", "filename": filename}
