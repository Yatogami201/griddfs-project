from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel
import secrets
import os
import aiohttp
import time
import threading
import logging
import asyncio
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from pathlib import Path

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
STORAGE_PATH = os.getenv("STORAGE_PATH", "/app/data")
HEARTBEAT_TIMEOUT = int(os.getenv("HEARTBEAT_TIMEOUT", "90"))
HEARTBEAT_CHECK_INTERVAL = int(os.getenv("HEARTBEAT_CHECK_INTERVAL", "30"))
TOKEN_EXPIRY_HOURS = int(os.getenv("TOKEN_EXPIRY_HOURS", "24"))
DEFAULT_BLOCK_SIZE = int(os.getenv("DEFAULT_BLOCK_SIZE", "67108864"))
MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", "10737418240"))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

_start_time = time.time()

app = FastAPI(
    title="GridDFS NameNode",
    description="NameNode central del sistema de archivos distribuido GridDFS",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)
Path(STORAGE_PATH).mkdir(parents=True, exist_ok=True)

files: Dict = {}
users: Dict = {}
directories: Dict = {}
datanodes: Dict = {}
datanode_status: Dict = {}
block_distribution_cache: Dict = {}


def log_namenode(message: str, level: str = "INFO"):
    timestamp = datetime.now().isoformat()
    log_entry = f"{timestamp} [NAMENODE-{level}] {message}"

    if level == "DEBUG" and LOG_LEVEL != "DEBUG":
        return

    print(log_entry)

    log_file = Path(STORAGE_PATH) / "namenode.log"
    try:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(log_entry + "\n")
    except Exception as e:
        logger.error(f"Error escribiendo log: {e}")


def check_datanode_status():
    """Periodically check DataNode status"""
    while True:
        time.sleep(30)
        current_time = datetime.now()
        inactive_nodes = []

        for dn_url in list(datanode_status.keys()):
            last_heartbeat = datanode_status[dn_url]["last_heartbeat"]
            if current_time - last_heartbeat > timedelta(seconds=HEARTBEAT_TIMEOUT):
                if datanode_status[dn_url]["status"] == "active":
                    datanode_status[dn_url]["status"] = "inactive"
                    inactive_nodes.append(dn_url)

        for node in inactive_nodes:
            log_namenode(f"DataNode {node} marcado como inactivo", "WARNING")


# Start status checker
status_thread = threading.Thread(target=check_datanode_status, daemon=True)
status_thread.start()


class DataNodeRegistration(BaseModel):
    datanode_url: str
    node_id: Optional[str] = "unknown"
    storage_capacity: Optional[int] = 0


class BlockInfo(BaseModel):
    index: int
    block_id: str
    datanode: str


class FileRegistration(BaseModel):
    filename: str
    size: int
    block_size: int
    blocks: List[BlockInfo]


class UserRegistration(BaseModel):
    username: str
    password: str


class HeartbeatData(BaseModel):
    datanode_url: str
    node_id: Optional[str] = "unknown"
    total_blocks: Optional[int] = 0
    storage_capacity: Optional[int] = 0


def get_current_user(token: str = Header(...)):
    """Obtener usuario actual basado en token"""
    for username, data in users.items():
        if data.get("token") == token:
            return username
    raise HTTPException(status_code=401, detail="Token inválido")


@app.get("/")
def root():
    """Health check y estado del sistema"""
    active_datanodes = []
    for dn_url, dn_info in datanodes.items():
        if datanode_status.get(dn_url, {}).get("status") == "active":
            active_datanodes.append({
                "url": dn_url,
                "node_id": dn_info["node_id"],
                "last_heartbeat": datanode_status[dn_url]["last_heartbeat"].isoformat(),
                "total_blocks": datanode_status[dn_url].get("total_blocks", 0)
            })

    return {
        "status": "namenode_active",
        "version": "1.0.0",
        "datanodes": active_datanodes,
        "total_files": len(files),
        "total_users": len(users),
        "total_directories": len(directories),
        "timestamp": datetime.now().isoformat()
    }


def get_active_datanodes() -> List[str]:
    """Obtener solo DataNodes activos"""
    current_time = time.time()
    return [dn_url for dn_url, status in datanode_status.items()
            if status.get("last_heartbeat", 0) > current_time - HEARTBEAT_TIMEOUT]


def cleanup_expired_tokens():
    """Limpiar tokens expirados"""
    current_time = datetime.now()
    expired_users = []
    for username, user_data in users.items():
        if "token_created" in user_data:
            token_time = datetime.fromisoformat(user_data["token_created"])
            if current_time - token_time > timedelta(hours=TOKEN_EXPIRY_HOURS):
                expired_users.append(username)

    for username in expired_users:
        users[username]["token"] = None
        users[username]["token_created"] = None


@app.post("/register_datanode")
def register_datanode(registration: DataNodeRegistration):
    """Registrar un nuevo DataNode"""
    datanode_url = registration.datanode_url

    # Registrar o actualizar información del DataNode
    if datanode_url not in datanodes:
        datanodes[datanode_url] = {
            "url": datanode_url,
            "node_id": registration.node_id,
            "storage_capacity": registration.storage_capacity,
            "registered_at": datetime.now().isoformat()
        }
        log_namenode(f"Nuevo DataNode registrado: {datanode_url} ({registration.node_id})")
    else:
        # Actualizar información existente
        datanodes[datanode_url].update({
            "node_id": registration.node_id,
            "storage_capacity": registration.storage_capacity
        })
        log_namenode(f"DataNode actualizado: {datanode_url}")

    # Marcar como activo
    datanode_status[datanode_url] = {
        "last_heartbeat": datetime.now(),
        "status": "active",
        "total_blocks": 0,
        "storage_capacity": registration.storage_capacity
    }

    return {
        "status": "registered",
        "datanode": datanode_url,
        "node_id": registration.node_id,
        "timestamp": datetime.now().isoformat()
    }


@app.post("/heartbeat")
def heartbeat(data: HeartbeatData):
    """Procesar heartbeat de DataNode"""
    datanode_url = data.datanode_url

    if datanode_url in datanode_status:
        # Actualizar información del heartbeat
        datanode_status[datanode_url].update({
            "last_heartbeat": datetime.now(),
            "status": "active",
            "total_blocks": data.total_blocks or 0,
            "storage_capacity": data.storage_capacity or 0
        })
    else:
        # Auto-registro si no existe
        datanodes[datanode_url] = {
            "url": datanode_url,
            "node_id": data.node_id or "unknown",
            "storage_capacity": data.storage_capacity or 0,
            "registered_at": datetime.now().isoformat()
        }
        datanode_status[datanode_url] = {
            "last_heartbeat": datetime.now(),
            "status": "active",
            "total_blocks": data.total_blocks or 0,
            "storage_capacity": data.storage_capacity or 0
        }
        log_namenode(f"DataNode auto-registrado vía heartbeat: {datanode_url}")

    return {
        "status": "acknowledged",
        "timestamp": datetime.now().isoformat()
    }


@app.get("/datanodes")
def get_datanodes(username: str = Depends(get_current_user)):
    """Obtener lista de DataNodes activos"""
    active_datanodes = []

    for dn_url, dn_info in datanodes.items():
        status_info = datanode_status.get(dn_url, {})
        if status_info.get("status") == "active":
            active_datanodes.append(dn_url)

    return {"datanodes": active_datanodes}


@app.get("/datanodes/detailed")
def get_datanodes_detailed(username: str = Depends(get_current_user)):
    """Obtener información detallada de DataNodes activos"""
    detailed_nodes = []

    for dn_url, dn_info in datanodes.items():
        status_info = datanode_status.get(dn_url, {})
        if status_info.get("status") == "active":
            detailed_nodes.append({
                "url": dn_url,
                "node_id": dn_info["node_id"],
                "storage_capacity": dn_info["storage_capacity"],
                "total_blocks": status_info.get("total_blocks", 0),
                "last_heartbeat": status_info["last_heartbeat"].isoformat(),
                "registered_at": dn_info["registered_at"]
            })

    return {"datanodes": detailed_nodes}


@app.post("/register_file")
def register_file(reg: FileRegistration, username: str = Depends(get_current_user)):
    """Registrar un nuevo archivo en el sistema"""
    normalized_filename = f"/{reg.filename}" if not reg.filename.startswith("/") else reg.filename
    normalized_filename = normalized_filename.replace("//", "/")

    # Verificar si el archivo ya existe
    key = (username, normalized_filename)
    if key in files:
        raise HTTPException(status_code=400, detail=f"File already exists: {normalized_filename}")

    # Crear directorios padre automáticamente
    parent_dir = os.path.dirname(normalized_filename)
    if parent_dir != "/" and parent_dir:
        dir_key = (username, parent_dir)
        if dir_key not in directories:
            directories[dir_key] = {
                "type": "directory",
                "owner": username,
                "created_at": datetime.now().isoformat()
            }
            log_namenode(f"Auto-created directory: {parent_dir} for user: {username}")

    # Validar que todos los bloques tienen DataNodes activos
    invalid_blocks = []
    for block in reg.blocks:
        if block.datanode not in [dn for dn in datanodes.keys()
                                  if datanode_status.get(dn, {}).get("status") == "active"]:
            invalid_blocks.append(block.block_id)

    if invalid_blocks:
        raise HTTPException(
            status_code=400,
            detail=f"Some blocks reference inactive DataNodes: {invalid_blocks}"
        )

    # Registrar el archivo
    files[key] = {
        "size": reg.size,
        "block_size": reg.block_size,
        "blocks": sorted([b.dict() for b in reg.blocks], key=lambda x: x["index"]),
        "created_at": datetime.now().isoformat(),
        "owner": username
    }

    log_namenode(f"File registered: {normalized_filename} by {username} ({len(reg.blocks)} blocks)")
    return {
        "msg": "Archivo registrado exitosamente",
        "filename": normalized_filename,
        "owner": username,
        "blocks": len(reg.blocks),
        "size": reg.size
    }


@app.get("/file/{filename:path}")
def get_file(filename: str, username: str = Depends(get_current_user)):
    """Obtener información de un archivo"""
    normalized_filename = f"/{filename}" if not filename.startswith("/") else filename
    normalized_filename = normalized_filename.replace("//", "/").rstrip("/")
    key = (username, normalized_filename)

    if key not in files:
        # Intentar formatos alternativos para compatibilidad
        alternatives = [
            (username, normalized_filename + "/"),
            (username, normalized_filename.lstrip("/")),
            (username, "/" + normalized_filename.lstrip("/"))
        ]

        for alt_key in alternatives:
            if alt_key in files:
                return files[alt_key]

        raise HTTPException(status_code=404, detail=f"File not found: {normalized_filename}")

    return files[key]


@app.delete("/rm/{filename:path}")
def rm(filename: str, username: str = Depends(get_current_user)):
    """Eliminar un archivo"""
    normalized_filename = f"/{filename}" if not filename.startswith("/") else filename
    normalized_filename = normalized_filename.replace("//", "/")
    key = (username, normalized_filename)

    if key not in files:
        raise HTTPException(status_code=404, detail=f"File not found: {normalized_filename}")

    # Obtener información de bloques antes de eliminar
    file_info = files[key]
    blocks_info = file_info.get("blocks", [])

    # Eliminar el archivo de metadatos
    del files[key]
    log_namenode(f"File deleted: {normalized_filename} by {username}")

    return {
        "msg": f"File removed: {normalized_filename}",
        "blocks_affected": len(blocks_info)
    }


@app.post("/register")
def register(user: UserRegistration):
    """Registrar nuevo usuario"""
    if user.username in users:
        raise HTTPException(status_code=400, detail="Usuario ya existe")

    if len(user.username) < 3:
        raise HTTPException(status_code=400, detail="Username debe tener al menos 3 caracteres")

    if len(user.password) < 6:
        raise HTTPException(status_code=400, detail="Password debe tener al menos 6 caracteres")

    users[user.username] = {
        "password": user.password,
        "token": None,
        "created_at": datetime.now().isoformat()
    }

    log_namenode(f"New user registered: {user.username}")
    return {"msg": "Usuario registrado exitosamente"}


@app.post("/login")
def login(user: UserRegistration):
    """Iniciar sesión"""
    if user.username not in users or users[user.username]["password"] != user.password:
        raise HTTPException(status_code=401, detail="Credenciales inválidas")

    token = secrets.token_hex(16)
    users[user.username]["token"] = token
    users[user.username]["last_login"] = datetime.now().isoformat()

    log_namenode(f"User logged in: {user.username}")
    return {"token": token}


@app.post("/mkdir/{path:path}")
def mkdir(path: str, username: str = Depends(get_current_user)):
    """Crear directorio"""
    dir_path = f"/{path}" if not path.startswith("/") else path
    dir_path = dir_path.replace("//", "/")
    key = (username, dir_path)

    if key in files or key in directories:
        raise HTTPException(status_code=400, detail=f"Path already exists: {dir_path}")

    directories[key] = {
        "type": "directory",
        "owner": username,
        "created_at": datetime.now().isoformat()
    }

    log_namenode(f"Directory created: {dir_path} by {username}")
    return {"msg": f"Directory created: {dir_path}"}


@app.delete("/rmdir/{path:path}")
def rmdir(path: str, username: str = Depends(get_current_user)):
    """Eliminar directorio"""
    dir_path = f"/{path}" if not path.startswith("/") else path
    dir_path = dir_path.replace("//", "/")
    key = (username, dir_path)

    if key not in directories:
        raise HTTPException(status_code=404, detail=f"Directory not found: {dir_path}")

    # Verificar que el directorio esté vacío
    dir_prefix = dir_path.rstrip("/") + "/"
    for (owner, item_path) in list(files.keys()) + list(directories.keys()):
        if owner == username and item_path.startswith(dir_prefix) and item_path != dir_path:
            raise HTTPException(status_code=400, detail=f"Directory not empty: {dir_path}")

    del directories[key]
    log_namenode(f"Directory removed: {dir_path} by {username}")
    return {"msg": f"Directory removed: {dir_path}"}


@app.get("/ls")
def ls(path: str = "/", username: str = Depends(get_current_user)):
    """Listar contenido de directorio"""
    if not path.startswith("/"):
        path = "/" + path
    path = path.replace("//", "/")

    # Verificar que el directorio existe o es root
    if path != "/" and (username, path) not in directories:
        raise HTTPException(status_code=404, detail=f"Directory not found: {path}")

    items = []
    path_prefix = path.rstrip("/") + "/" if path != "/" else "/"

    # Añadir directorios
    for (owner, dir_path), meta in directories.items():
        if owner == username and dir_path.startswith(path_prefix) and dir_path != path:
            relative_path = dir_path[len(path_prefix):].rstrip("/")
            if "/" not in relative_path and relative_path:  # Solo hijos inmediatos
                items.append({
                    "name": relative_path,
                    "type": "directory",
                    "size": 0,
                    "created_at": meta.get("created_at", "")
                })

    # Añadir archivos
    for (owner, file_path), meta in files.items():
        if owner == username and file_path.startswith(path_prefix):
            relative_path = file_path[len(path_prefix):]
            if "/" not in relative_path and relative_path:  # Solo hijos inmediatos
                items.append({
                    "name": relative_path,
                    "type": "file",
                    "size": meta["size"],
                    "block_size": meta["block_size"],
                    "blocks": len(meta["blocks"]),
                    "created_at": meta.get("created_at", "")
                })

    return {"path": path, "items": items}


async def verify_block_exists(block_info: Dict) -> bool:
    """Verificar que un bloque existe en su DataNode"""
    try:
        if not all(key in block_info for key in ['datanode', 'block_id']):
            return False

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.get(f"{block_info['datanode']}/block/{block_info['block_id']}") as response:
                return response.status == 200
    except Exception as e:
        logger.error(f"Error verificando bloque {block_info.get('block_id', 'unknown')}: {e}")
        return False


@app.get("/file_health/{filename:path}")
async def check_file_health(filename: str, username: str = Depends(get_current_user)):
    """Verificar integridad de un archivo"""
    normalized_filename = f"/{filename}" if not filename.startswith("/") else filename
    normalized_filename = normalized_filename.replace("//", "/").rstrip("/")
    key = (username, normalized_filename)

    if key not in files:
        raise HTTPException(status_code=404, detail=f"File not found: {normalized_filename}")

    file_info = files[key]
    missing_blocks = []

    # Verificar cada bloque asíncronamente
    verification_tasks = [verify_block_exists(block) for block in file_info["blocks"]]
    results = await asyncio.gather(*verification_tasks, return_exceptions=True)

    for i, (block, result) in enumerate(zip(file_info["blocks"], results)):
        if isinstance(result, Exception) or not result:
            missing_blocks.append({
                "index": i,
                "block_id": block["block_id"],
                "datanode": block["datanode"]
            })

    return {
        "filename": normalized_filename,
        "total_blocks": len(file_info["blocks"]),
        "missing_blocks": missing_blocks,
        "healthy": len(missing_blocks) == 0,
        "integrity_score": (len(file_info["blocks"]) - len(missing_blocks)) / len(file_info["blocks"]) * 100
    }


@app.get("/system_status")
def get_system_status(username: str = Depends(get_current_user)):
    """Obtener estado completo del sistema"""
    active_nodes = sum(1 for status in datanode_status.values() if status.get("status") == "active")
    inactive_nodes = len(datanode_status) - active_nodes

    total_blocks = sum(status.get("total_blocks", 0) for status in datanode_status.values()
                       if status.get("status") == "active")

    user_files = len([f for f in files.keys() if f[0] == username])
    user_dirs = len([d for d in directories.keys() if d[0] == username])

    return {
        "system": {
            "status": "healthy",
            "uptime": "running",  # En producción calcularía el uptime real
            "timestamp": datetime.now().isoformat()
        },
        "datanodes": {
            "total": len(datanodes),
            "active": active_nodes,
            "inactive": inactive_nodes
        },
        "storage": {
            "total_blocks": total_blocks,
            "total_files": len(files),
            "total_directories": len(directories)
        },
        "user_stats": {
            "files": user_files,
            "directories": user_dirs
        }
    }


@app.get("/distribution_plan")
def get_distribution_plan(num_blocks: int, username: str = Depends(get_current_user)):
    """Obtener plan de distribución para n bloques"""
    if num_blocks <= 0:
        raise HTTPException(status_code=400, detail="Número de bloques debe ser positivo")

    active_nodes = get_active_datanodes()
    if not active_nodes:
        raise HTTPException(status_code=503, detail="No hay DataNodes activos")

    distribution = [active_nodes[i % len(active_nodes)] for i in range(num_blocks)]
    return {"distribution": distribution, "block_count": num_blocks}