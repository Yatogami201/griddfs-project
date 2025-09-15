#!/usr/bin/env python3
"""
GridDFS Client - Sistema de Archivos Distribuido
Cliente independiente que se conecta al cluster GridDFS via HTTP/REST
"""
import os
import sys
import json
import math
import uuid
import click
import requests
from urllib.parse import quote
from dotenv import load_dotenv
from pathlib import Path
import time
from typing import List, Dict, Optional

# Cargar configuración
load_dotenv()

# Configuración desde variables de entorno
NAMENODE_URL = os.getenv("NAMENODE_URL", "http://localhost:5000")
DEFAULT_BLOCK_SIZE_MB = int(os.getenv("BLOCK_SIZE_MB", "4"))
DEFAULT_BLOCK_SIZE = DEFAULT_BLOCK_SIZE_MB * 1024 * 1024
TOKEN_FILE = os.getenv("TOKEN_FILE", ".griddfs_token")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

class GridDFSClient:
    
    def __init__(self):
        self.namenode_url = NAMENODE_URL
        self.token_file = TOKEN_FILE
        
    def save_token(self, token: str):
        """Guardar token de autenticación"""
        try:
            with open(self.token_file, "w") as f:
                f.write(token)
        except Exception as e:
            click.echo(f"Error guardando token: {e}")
            
    def load_token(self) -> Optional[str]:
        """Cargar token de autenticación"""
        try:
            if os.path.exists(self.token_file):
                with open(self.token_file, "r") as f:
                    return f.read().strip()
        except Exception as e:
            click.echo(f"Error cargando token: {e}")
        return None
        
    def auth_headers(self) -> Dict[str, str]:
        """Obtener headers de autenticación"""
        token = self.load_token()
        if not token:
            raise click.ClickException("No estás logueado. Usa 'griddfs login' primero.")
        return {"token": token}
        
    def check_connection(self) -> bool:
        """Verificar conexión con NameNode"""
        try:
            response = requests.get(f"{self.namenode_url}/", timeout=10)
            return response.status_code == 200
        except Exception:
            return False
            
    def make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Hacer petición HTTP con reintentos"""
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.request(method, url, timeout=30, **kwargs)
                return response
            except requests.exceptions.ConnectionError:
                if attempt == MAX_RETRIES - 1:
                    raise click.ClickException("No se puede conectar al NameNode. Verifica que esté ejecutándose.")
                time.sleep(2)
            except requests.exceptions.Timeout:
                if attempt == MAX_RETRIES - 1:
                    raise click.ClickException("Timeout conectando al NameNode.")
                time.sleep(2)
        
    def get_datanodes(self) -> List[str]:
        """Obtener lista de DataNodes activos"""
        response = self.make_request("GET", f"{self.namenode_url}/datanodes", headers=self.auth_headers())
        response.raise_for_status()
        return response.json().get("datanodes", [])

# Instancia global del cliente
client = GridDFSClient()

@click.group()
@click.version_option(version="1.0.0")
def cli():
 
    pass

@cli.command()
def ping():
    """Verificar conexión con el NameNode"""
    try:
        response = client.make_request("GET", f"{client.namenode_url}/")
        if response.status_code == 200:
            data = response.json()
            click.echo("Conexión exitosa con NameNode")
            click.echo(f"Estado: {data.get('status')}")
            click.echo(f"DataNodes activos: {len(data.get('datanodes', []))}")
            click.echo(f"Archivos totales: {data.get('total_files')}")
            click.echo(f"Usuarios totales: {data.get('total_users')}")
        else:
            click.echo(f"Error: {response.status_code} - {response.text}")
    except Exception as e:
        click.echo(f"Error de conexión: {e}")

@cli.command()
@click.argument("username")
@click.argument("password")
def register(username: str, password: str):
    """Registrar nuevo usuario"""
    try:
        response = client.make_request(
            "POST",
            f"{client.namenode_url}/register",
            json={"username": username, "password": password}
        )
        if response.status_code == 200:
            click.echo("Usuario registrado correctamente")
        else:
            click.echo(f"Error en registro: {response.text}")
    except Exception as e:
        click.echo(f"Error: {e}")

@cli.command()
@click.argument("username")
@click.argument("password")
def login(username: str, password: str):
    """Iniciar sesión"""
    try:
        response = client.make_request(
            "POST",
            f"{client.namenode_url}/login",
            json={"username": username, "password": password}
        )
        if response.status_code == 200:
            token = response.json().get("token")
            client.save_token(token)
            click.echo("Login exitoso. Token guardado.")
        else:
            click.echo(f"Error de autenticación: {response.text}")
    except Exception as e:
        click.echo(f"Error: {e}")

@cli.command()
@click.argument("local_path", type=click.Path(exists=True, dir_okay=False))
@click.argument("remote_name", required=False)
@click.option("--block-size", type=int, default=DEFAULT_BLOCK_SIZE, 
              help=f"Tamaño de bloque en bytes (por defecto: {DEFAULT_BLOCK_SIZE})")
@click.option("--progress/--no-progress", default=True, help="Mostrar progreso de subida")
def put(local_path: str, remote_name: Optional[str], block_size: int, progress: bool):
    """Subir archivo al sistema distribuido"""
    try:
        # Verificar conexión
        if not client.check_connection():
            raise click.ClickException("No hay conexión con el NameNode")
            
        if not remote_name:
            remote_name = os.path.basename(local_path)
        
        # Obtener DataNodes activos
        datanodes = client.get_datanodes()
        if not datanodes:
            raise click.ClickException("No hay DataNodes activos")
        
        file_size = os.path.getsize(local_path)
        total_blocks = math.ceil(file_size / block_size)
        blocks_meta = []
        
        if progress:
            click.echo(f"Subiendo '{local_path}' como '{remote_name}'")
            click.echo(f"Tamaño: {file_size:,} bytes")
            click.echo(f"Bloques: {total_blocks}")
            click.echo(f"Tamaño de bloque: {block_size:,} bytes")
            click.echo(f"DataNodes disponibles: {len(datanodes)}")
        
        # Subir bloques usando round-robin
        with open(local_path, "rb") as f:
            for i in range(total_blocks):
                chunk = f.read(block_size)
                if not chunk:
                    break
                    
                block_id = f"{remote_name}__{i}__{uuid.uuid4().hex}"
                safe_block_id = block_id.replace("/", "_").replace("\\", "_")
                
                # Seleccionar DataNode usando round-robin
                datanode = datanodes[i % len(datanodes)]
                
                # Subir bloque
                files = {"file": (safe_block_id, chunk, "application/octet-stream")}
                upload_url = f"{datanode}/upload_block/{safe_block_id}"
                
                upload_response = requests.post(upload_url, files=files, timeout=120)
                if upload_response.status_code != 200:
                    raise click.ClickException(f"Error subiendo bloque {i} a {datanode}: {upload_response.text}")
                
                blocks_meta.append({
                    "index": i,
                    "block_id": safe_block_id,
                    "datanode": datanode
                })
                
                if progress:
                    click.echo(f"  Bloque {i+1}/{total_blocks} -> {datanode} ✓")
        
        # Registrar archivo en NameNode
        registration = {
            "filename": remote_name,
            "size": file_size,
            "block_size": block_size,
            "blocks": blocks_meta
        }
        
        reg_response = client.make_request(
            "POST",
            f"{client.namenode_url}/register_file",
            json=registration,
            headers=client.auth_headers()
        )
        reg_response.raise_for_status()
        
        click.echo("Archivo subido exitosamente")
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 400:
            click.echo(f"Error: El archivo '{remote_name}' ya existe")
        else:
            click.echo(f"Error HTTP: {e}")
    except Exception as e:
        click.echo(f"Error: {e}")

@cli.command()
@click.argument("remote_name")
@click.argument("output_path", type=click.Path(dir_okay=False))
@click.option("--progress/--no-progress", default=True, help="Mostrar progreso de descarga")
def get(remote_name: str, output_path: str, progress: bool):
    """Descargar archivo del sistema distribuido"""
    try:
        # Obtener metadatos del archivo
        encoded_name = quote(remote_name, safe='')
        meta_response = client.make_request(
            "GET",
            f"{client.namenode_url}/file/{encoded_name}",
            headers=client.auth_headers()
        )
        
        if meta_response.status_code != 200:
            raise click.ClickException(f"Archivo no encontrado: {remote_name}")
        
        metadata = meta_response.json()
        blocks = sorted(metadata.get("blocks", []), key=lambda b: b["index"])
        
        if not blocks:
            raise click.ClickException("No hay bloques para este archivo")
        
        if progress:
            click.echo(f"Descargando '{remote_name}' -> '{output_path}'")
            click.echo(f"Bloques a descargar: {len(blocks)}")
        
        # Descargar y reconstruir archivo
        with open(output_path, "wb") as out_file:
            for i, block in enumerate(blocks):
                block_url = f"{block['datanode']}/block/{block['block_id']}"
                
                block_response = requests.get(block_url, stream=True, timeout=120)
                if block_response.status_code != 200:
                    raise click.ClickException(f"Error descargando bloque {i} desde {block_url}")
                
                block_size = 0
                for chunk in block_response.iter_content(chunk_size=1024*1024):
                    if chunk:
                        out_file.write(chunk)
                        block_size += len(chunk)
                
                if progress:
                    click.echo(f"  Bloque {i+1}/{len(blocks)} ({block_size:,} bytes) ✓")
        
        click.echo("Descarga completada")
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            click.echo(f"Error: Archivo '{remote_name}' no encontrado")
        else:
            click.echo(f"Error HTTP: {e}")
    except Exception as e:
        click.echo(f"Error: {e}")

@cli.command()
@click.argument("dirname")
def mkdir(dirname: str):
    """Crear directorio"""
    try:
        response = client.make_request(
            "POST",
            f"{client.namenode_url}/mkdir/{dirname}",
            headers=client.auth_headers()
        )
        if response.status_code == 200:
            click.echo(f"Directorio creado: {dirname}")
        else:
            click.echo(f"Error: {response.text}")
    except Exception as e:
        click.echo(f"Error: {e}")

@cli.command()
@click.argument("dirname")
def rmdir(dirname: str):
    """Eliminar directorio vacío"""
    try:
        response = client.make_request(
            "DELETE",
            f"{client.namenode_url}/rmdir/{dirname}",
            headers=client.auth_headers()
        )
        if response.status_code == 200:
            click.echo(f"Directorio eliminado: {dirname}")
        else:
            click.echo(f"Error: {response.text}")
    except Exception as e:
        click.echo(f"Error: {e}")

@cli.command()
@click.argument("filename")
def rm(filename: str):
    """Eliminar archivo"""
    try:
        response = client.make_request(
            "DELETE",
            f"{client.namenode_url}/rm/{filename}",
            headers=client.auth_headers()
        )
        if response.status_code == 200:
            click.echo(f"Archivo eliminado: {filename}")
        else:
            click.echo(f"Error: {response.text}")
    except Exception as e:
        click.echo(f"Error: {e}")

@cli.command()
@click.argument("path", required=False, default="/")
@click.option("--detailed", "-l", is_flag=True, help="Mostrar información detallada")
def ls(path: str, detailed: bool):
    """Listar archivos y directorios"""
    try:
        response = client.make_request(
            "GET",
            f"{client.namenode_url}/ls?path={path}",
            headers=client.auth_headers()
        )
        
        if response.status_code != 200:
            click.echo(f"Error: {response.text}")
            return
            
        data = response.json()
        click.echo(f"Contenido de {data['path']}:")
        
        items = data.get("items", [])
        if not items:
            click.echo("  (vacío)")
            return
        
        for item in items:
            if item["type"] == "directory":
                click.echo(f"  📂 {item['name']}/")
            else:
                if detailed:
                    click.echo(f"  📄 {item['name']} ({item['size']:,} bytes, "
                             f"{item['blocks']} bloques, bloque: {item['block_size']:,} bytes)")
                else:
                    click.echo(f"  📄 {item['name']} ({item['size']:,} bytes)")
                    
    except Exception as e:
        click.echo(f"Error: {e}")

@cli.command()
@click.option("--detailed", "-d", is_flag=True, help="Información detallada")
def datanodes(detailed: bool):
    """Listar DataNodes activos"""
    try:
        if detailed:
            endpoint = "/datanodes/detailed"
        else:
            endpoint = "/datanodes"
            
        response = client.make_request(
            "GET",
            f"{client.namenode_url}{endpoint}",
            headers=client.auth_headers()
        )
        
        if response.status_code != 200:
            click.echo(f"Error: {response.text}")
            return
            
        data = response.json()
        datanodes_list = data.get("datanodes", [])
        
        if not datanodes_list:
            click.echo("No hay DataNodes activos")
            return
            
        click.echo(f"DataNodes activos ({len(datanodes_list)}):")
        
        for i, node in enumerate(datanodes_list, 1):
            if detailed and isinstance(node, dict):
                click.echo(f"  {i}. {node['url']}")
                click.echo(f"     ID: {node['node_id']}")
                click.echo(f"     Bloques: {node['total_blocks']}")
                click.echo(f"     Último heartbeat: {node['last_heartbeat']}")
            else:
                # Para compatibilidad con formato simple
                node_url = node if isinstance(node, str) else node.get('url', str(node))
                try:
                    # Intentar obtener info directa del DataNode
                    node_response = requests.get(f"{node_url}/", timeout=5)
                    if node_response.status_code == 200:
                        node_info = node_response.json()
                        click.echo(f"  {i}. {node_url}")
                        click.echo(f"     Bloques: {node_info.get('total_blocks', 'N/A')}")
                        click.echo(f"     Tamaño: {node_info.get('total_size', 'N/A'):,} bytes")
                        click.echo(f"     Estado: {node_info.get('status', 'N/A')}")
                    else:
                        click.echo(f"  {i}. {node_url} (No responde)")
                except:
                    click.echo(f"  {i}. {node_url} (Error de conexión)")
                    
    except Exception as e:
        click.echo(f"Error: {e}")

@cli.command()
@click.argument("filename")
def health(filename: str):
    """Verificar integridad de un archivo"""
    try:
        encoded_name = quote(filename, safe='')
        response = client.make_request(
            "GET",
            f"{client.namenode_url}/file_health/{encoded_name}",
            headers=client.auth_headers()
        )
        
        if response.status_code != 200:
            click.echo(f"Error verificando archivo: {response.text}")
            return
            
        health_info = response.json()
        
        if health_info["healthy"]:
            click.echo(f"Archivo '{filename}' está saludable")
            click.echo(f"Todos los {health_info['total_blocks']} bloques están presentes")
            click.echo(f"Integridad: {health_info.get('integrity_score', 100):.1f}%")
        else:
            click.echo(f"Archivo '{filename}' tiene problemas de integridad")
            click.echo(f"Bloques faltantes: {len(health_info['missing_blocks'])}/{health_info['total_blocks']}")
            click.echo(f"Integridad: {health_info.get('integrity_score', 0):.1f}%")
            
            for missing in health_info["missing_blocks"]:
                click.echo(f"  Bloque {missing['index']} faltante en {missing['datanode']}")
                
    except Exception as e:
        click.echo(f"Error: {e}")

@cli.command()
def status():
    """Mostrar estado del sistema"""
    try:
        response = client.make_request(
            "GET",
            f"{client.namenode_url}/system_status",
            headers=client.auth_headers()
        )
        
        if response.status_code != 200:
            click.echo(f"Error obteniendo estado: {response.text}")
            return
            
        status_data = response.json()
        
        click.echo("Estado del Sistema GridDFS")
        click.echo("=" * 30)
        
        system = status_data.get("system", {})
        click.echo(f"Sistema: {system.get('status', 'unknown')}")
        
        datanodes_info = status_data.get("datanodes", {})
        click.echo(f"DataNodes: {datanodes_info.get('active', 0)} activos, "
                  f"{datanodes_info.get('inactive', 0)} inactivos")
        
        storage = status_data.get("storage", {})
        click.echo(f"Almacenamiento: {storage.get('total_blocks', 0)} bloques totales")
        click.echo(f"Archivos: {storage.get('total_files', 0)} totales")
        
        user_stats = status_data.get("user_stats", {})
        click.echo(f"Tus archivos: {user_stats.get('files', 0)}")
        click.echo(f"Tus directorios: {user_stats.get('directories', 0)}")
        
    except Exception as e:
        click.echo(f"Error: {e}")

@cli.command()
def cleanup():
    """Limpiar token de autenticación"""
    try:
        if os.path.exists(client.token_file):
            os.remove(client.token_file)
            click.echo("Token eliminado exitosamente")
        else:
            click.echo("No hay token para limpiar")
    except Exception as e:
        click.echo(f"Error: {e}")

@cli.command()
def config():
    """Mostrar configuración actual del cliente"""
    click.echo("Configuración GridDFS Client")
    click.echo("=" * 30)
    click.echo(f"NameNode URL: {client.namenode_url}")
    click.echo(f"Tamaño de bloque por defecto: {DEFAULT_BLOCK_SIZE_MB} MB")
    click.echo(f"Archivo de token: {client.token_file}")
    click.echo(f"Máximos reintentos: {MAX_RETRIES}")
    
    # Verificar estado de autenticación
    token = client.load_token()
    if token:
        click.echo("Estado: Autenticado")
        # Verificar si el token es válido
        try:
            response = requests.get(f"{client.namenode_url}/ls", 
                                  headers={"token": token}, timeout=5)
            if response.status_code == 200:
                click.echo("Token: Válido")
            else:
                click.echo("Token: Inválido (reautenticación necesaria)")
        except:
            click.echo("Token: No se puede verificar")
    else:
        click.echo("Estado: No autenticado")
    
    # Verificar conexión con NameNode
    if client.check_connection():
        click.echo("Conexión NameNode: OK")
    else:
        click.echo("Conexión NameNode: FALLO")

if __name__ == "__main__":

    cli()
