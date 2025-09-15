from fastapi import FastAPI, UploadFile, HTTPException
from fastapi import FastAPI, UploadFile, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from contextlib import asynccontextmanager
import os
import time
import threading
import requests
from datetime import datetime
import shutil
import logging
import hashlib
from pathlib import Path

# Configurar logging con nivel configurable
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración desde variables de entorno con valores por defecto mejorados
NODE_ID = os.getenv("NODE_ID", "datanode_unknown")
STORAGE_ROOT = os.getenv("STORAGE_ROOT", "/data/storage")
LOGS_PATH = os.getenv("LOGS_PATH", "/data/logs")
DATANODE_URL = os.getenv("DATANODE_URL", "http://localhost:5000")
NAMENODE_URL = os.getenv("NAMENODE_URL", "http://namenode:5000")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "20"))
EXTERNAL_URL = os.getenv("EXTERNAL_URL", DATANODE_URL)

# Crear estructura de directorios
for path in [STORAGE_ROOT, LOGS_PATH]:
    Path(path).mkdir(parents=True, exist_ok=True)

def log_message(message: str, level: str = "INFO"):
    """Log messages to stdout and file with timestamp"""
    timestamp = datetime.now().isoformat()
    log_entry = f"{timestamp} [{level}] {NODE_ID}: {message}"
    print(log_entry)
    
    # También escribir a archivo de log
    log_file = Path(LOGS_PATH) / f"{NODE_ID}.log"
    try:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(log_entry + "\n")
    except Exception as e:
        logger.error(f"Error escribiendo log: {e}")

def register_with_namenode() -> bool:
    """Register with NameNode with improved retry logic"""
    retry_delay = 3
    
    for attempt in range(MAX_RETRIES):
        try:
            log_message(f"Intento {attempt + 1}/{MAX_RETRIES} de registro con NameNode...")
            
            # Verificar primero si NameNode responde
            health_check = requests.get(f"{NAMENODE_URL}/", timeout=10)
            if health_check.status_code != 200:
                log_message(f"NameNode no responde aún (HTTP {health_check.status_code})", "WARNING")
                time.sleep(retry_delay)
                continue
            
            # Obtener información del sistema para el registro
            storage_capacity = get_available_storage()
            
            # Intentar registro con información detallada
            registration_data = {
                "datanode_url": DATANODE_URL,
                "node_id": NODE_ID,
                "storage_capacity": storage_capacity
            }
            
            response = requests.post(
                f"{NAMENODE_URL}/register_datanode",
                json=registration_data,
                timeout=15
            )
            
            if response.status_code == 200:
                log_message(f"Registrado exitosamente con NameNode: {DATANODE_URL}")
                log_message(f"Capacidad de almacenamiento reportada: {storage_capacity} bytes")
                return True
            else:
                log_message(f"Registro falló (HTTP {response.status_code}): {response.text}", "ERROR")
                
        except requests.exceptions.ConnectionError:
            log_message(f"NameNode no disponible (intento {attempt + 1}), reintentando...", "WARNING")
        except requests.exceptions.Timeout:
            log_message(f"Timeout conectando con NameNode (intento {attempt + 1})", "WARNING")
        except Exception as e:
            log_message(f"Error inesperado en registro (intento {attempt + 1}): {e}", "ERROR")
        
        time.sleep(min(retry_delay * (attempt + 1), 30))  # Backoff exponencial limitado
    
    log_message("No se pudo registrar con NameNode después de todos los intentos", "ERROR")
    return False

def start_heartbeat():
    """Start heartbeat thread with improved error handling"""
    def heartbeat_loop():
        consecutive_failures = 0
        max_failures = 5
        
        while True:
            try:
                heartbeat_data = {
                    "datanode_url": DATANODE_URL,
                    "node_id": NODE_ID,
                    "total_blocks": count_blocks(),
                    "storage_capacity": get_available_storage()
                }
                
                response = requests.post(
                    f"{NAMENODE_URL}/heartbeat",
                    json=heartbeat_data,
                    timeout=10
                )
                
                if response.status_code == 200:
                    if consecutive_failures > 0:
                        log_message("Heartbeat restablecido")
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                    log_message(f"Heartbeat falló (HTTP {response.status_code})", "WARNING")
                    
            except Exception as e:
                consecutive_failures += 1
                log_message(f"Error enviando heartbeat: {e}", "ERROR")
                
                # Si hay muchos fallos consecutivos, intentar re-registro
                if consecutive_failures >= max_failures:
                    log_message("Demasiados fallos de heartbeat, intentando re-registro...", "WARNING")
                    if register_with_namenode():
                        consecutive_failures = 0
            
            time.sleep(30)
    
    heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    heartbeat_thread.start()
    log_message("Heartbeat iniciado")

def get_available_storage() -> int:
    """Get available storage space in bytes"""
    try:
        return shutil.disk_usage(STORAGE_ROOT).free
    except Exception:
        return 0

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager para startup/shutdown"""
    # ==================== STARTUP ====================
    log_message(f"Iniciando DataNode: {NODE_ID}")
    log_message(f"Storage root: {STORAGE_ROOT}")
    log_message(f"Conectando a NameNode: {NAMENODE_URL}")
    log_message(f"URL de este DataNode: {DATANODE_URL}")
    
    # Crear estructura inicial de directorios
    init_directories = ["blocks", "temp", "backups", "metadata"]
    for directory in init_directories:
        dir_path = Path(STORAGE_ROOT) / directory
        dir_path.mkdir(parents=True, exist_ok=True)
        log_message(f"Directorio creado: {dir_path}")
    
    # Intentar registro con NameNode
    if register_with_namenode():
        start_heartbeat()
        log_message("DataNode inicializado y registrado exitosamente")
    else:
        log_message("DataNode funcionando en modo desconectado (sin NameNode)", "WARNING")
    
    yield  # La aplicación corre aquí
    
    # ==================== SHUTDOWN ====================
    log_message("DataNode shutting down...")

# Crear la app con lifespan
app = FastAPI(
    title=f"GridDFS DataNode - {NODE_ID}",
    description="DataNode del sistema de archivos distribuido GridDFS",
    version="1.0.0",
    lifespan=lifespan
)

# ==================== REST API ENDPOINTS ====================
@app.get("/")
def health():
    """Health check endpoint"""
    return {
        "node_id": NODE_ID,
        "storage_root": STORAGE_ROOT,
        "total_blocks": count_blocks(),
        "total_size": get_storage_size(),
        "free_space": get_available_storage(),
        "status": "healthy",
        "registered": is_registered_with_namenode(),
        "timestamp": datetime.now().isoformat()
    }

def is_registered_with_namenode() -> bool:
    """Check if this datanode is registered with namenode"""
    try:
        response = requests.get(f"{NAMENODE_URL}/", timeout=5)
        if response.status_code == 200:
            data = response.json()
            active_nodes = data.get("datanodes", [])
            return any(node.get("url") == DATANODE_URL for node in active_nodes)
    except Exception:
        pass
    return False

def count_blocks() -> int:
    """Count number of blocks stored"""
    blocks_dir = Path(STORAGE_ROOT) / "blocks"
    if blocks_dir.exists():
        return len([f for f in blocks_dir.iterdir() if f.is_file()])
    return 0

def get_storage_size() -> int:
    """Get total storage size in bytes"""
    total_size = 0
    blocks_dir = Path(STORAGE_ROOT) / "blocks"
    if blocks_dir.exists():
        for file_path in blocks_dir.iterdir():
            if file_path.is_file():
                try:
                    total_size += file_path.stat().st_size
                except Exception:
                    continue
    return total_size

@app.post("/upload_block/{block_id}")
async def upload_block(block_id: str, file: UploadFile):
    """Upload a block to this DataNode"""
    # Sanitizar el block_id
    safe_block_id = block_id.replace("/", "_").replace("..", "").replace("\\", "_")
    block_path = Path(STORAGE_ROOT) / "blocks" / safe_block_id
    
    try:
        # Asegurar que el directorio padre existe
        block_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Escribir el archivo por chunks para manejar archivos grandes
        with open(block_path, "wb") as f:
            while True:
                chunk = await file.read(1024 * 1024)  # 1MB chunks
                if not chunk:
                    break
                f.write(chunk)
        
        # Calcular hash para verificación
        file_hash = calculate_block_hash(str(block_path))
        file_size = block_path.stat().st_size
        
        log_message(f"Bloque almacenado: {safe_block_id} ({file_size} bytes, hash: {file_hash[:8]}...)")
        
        return {
            "block_id": safe_block_id,
            "size": file_size,
            "hash": file_hash,
            "datanode": DATANODE_URL,
            "storage_path": str(block_path),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        log_message(f"Error subiendo bloque {block_id}: {e}", "ERROR")
        # Limpiar archivo parcial si existe
        if block_path.exists():
            try:
                block_path.unlink()
            except Exception:
                pass
        raise HTTPException(status_code=500, detail=f"Error storing block: {e}")

@app.get("/block/{block_id}")
def get_block(block_id: str):
    """Retrieve a block from this DataNode"""
    safe_block_id = block_id.replace("/", "_").replace("..", "").replace("\\", "_")
    block_path = Path(STORAGE_ROOT) / "blocks" / safe_block_id
    
    if not block_path.exists():
        log_message(f"Bloque no encontrado: {safe_block_id}", "WARNING")
        raise HTTPException(status_code=404, detail="Block not found")
    
    if not block_path.is_file():
        log_message(f"Ruta no es un archivo: {safe_block_id}", "ERROR")
        raise HTTPException(status_code=404, detail="Block not found")
    
    log_message(f"Enviando bloque: {safe_block_id}")
    return FileResponse(
        str(block_path), 
        media_type="application/octet-stream",
        filename=safe_block_id
    )

@app.get("/storage_info")
def get_storage_info():
    """Get detailed storage information"""
    blocks_dir = Path(STORAGE_ROOT) / "blocks"
    blocks = []
    
    if blocks_dir.exists():
        for block_file in blocks_dir.iterdir():
            if block_file.is_file():
                try:
                    stat = block_file.stat()
                    blocks.append({
                        "block_id": block_file.name,
                        "size": stat.st_size,
                        "created": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                        "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
                    })
                except Exception:
                    continue
    
    return {
        "node_id": NODE_ID,
        "storage_root": STORAGE_ROOT,
        "total_blocks": len(blocks),
        "total_size": sum(block["size"] for block in blocks),
        "blocks": blocks,
        "free_space": get_available_storage(),
        "registered": is_registered_with_namenode(),
        "timestamp": datetime.now().isoformat()
    }

@app.post("/reregister")
def reregister():
    """Force re-registration with NameNode"""
    log_message("Forzando re-registro con NameNode...")
    success = register_with_namenode()
    if success:
        # El heartbeat ya está corriendo, no necesita reiniciarse
        log_message("Re-registro completado exitosamente")
    return {"success": success, "message": "Re-registration attempted"}

def calculate_block_hash(file_path: str) -> str:
    """Calcular hash MD5 de un bloque para verificación de integridad"""
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception:
        return "error"

@app.get("/block_info/{block_id}")
def get_block_info(block_id: str):
    """Obtener información detallada de un bloque"""
    safe_block_id = block_id.replace("/", "_").replace("..", "").replace("\\", "_")
    block_path = Path(STORAGE_ROOT) / "blocks" / safe_block_id
    
    if not block_path.exists():
        raise HTTPException(status_code=404, detail="Block not found")
    
    try:
        stat = block_path.stat()
        return {
            "block_id": safe_block_id,
            "size": stat.st_size,
            "hash": calculate_block_hash(str(block_path)),
            "created": datetime.fromtimestamp(stat.st_ctime).isoformat(),
            "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            "datanode": DATANODE_URL
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting block info: {e}")

@app.delete("/block/{block_id}")
def delete_block(block_id: str):
    """Eliminar un bloque (para operaciones de mantenimiento)"""
    safe_block_id = block_id.replace("/", "_").replace("..", "").replace("\\", "_")
    block_path = Path(STORAGE_ROOT) / "blocks" / safe_block_id
    
    if not block_path.exists():
        raise HTTPException(status_code=404, detail="Block not found")
    
    try:
        block_path.unlink()
        log_message(f"Bloque eliminado: {safe_block_id}")
        
        return {
            "status": "deleted", 
            "block_id": safe_block_id,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        log_message(f"Error eliminando bloque {safe_block_id}: {e}", "ERROR")
        raise HTTPException(status_code=500, detail=f"Error deleting block: {e}")