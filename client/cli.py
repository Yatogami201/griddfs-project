import os
import math
import uuid
import click
import requests
from urllib.parse import quote

NAMENODE_URL = os.getenv("NAMENODE_URL", "http://namenode:5000")
DEFAULT_BLOCK_SIZE_MB = int(os.getenv("BLOCK_SIZE_MB", "4"))  # ajústalo (p.ej. 64)
DEFAULT_BLOCK_SIZE = DEFAULT_BLOCK_SIZE_MB * 1024 * 1024

# ======================
# Manejo de Token
# ======================
TOKEN_FILE = ".token"

def save_token(token: str):
    with open(TOKEN_FILE, "w") as f:
        f.write(token)

def load_token():
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            return f.read().strip()
    return None

def auth_headers():
    token = load_token()
    if not token:
        raise click.ClickException("No estás logueado. Usa el comando 'login' primero.")
    return {"token": token}

# ======================
# CLI principal
# ======================
@click.group()
def cli():
    """GridDFS CLI"""
    pass

@cli.command()
def ping():
    """Probar conexión al NameNode"""
    r = requests.get(f"{NAMENODE_URL}/")
    click.echo(r.json())

@cli.command("register")
@click.argument("username")
@click.argument("password")
def register(username, password):
    """Registrar un nuevo usuario"""
    url = f"{NAMENODE_URL}/register"
    r = requests.post(url, json={"username": username, "password": password})
    if r.status_code != 200:
        raise click.ClickException(f"Error en registro: {r.text}")
    click.echo("Usuario registrado correctamente.")

@cli.command("login")
@click.argument("username")
@click.argument("password")
def login(username, password):
    """Iniciar sesión y guardar token"""
    url = f"{NAMENODE_URL}/login"
    r = requests.post(url, json={"username": username, "password": password})
    if r.status_code != 200:
        raise click.ClickException(f"Error al iniciar sesión: {r.text}")
    token = r.json().get("token")
    save_token(token)
    click.echo("Login exitoso. Token guardado.")


@cli.command("put")
@click.argument("local_path", type=click.Path(exists=True, dir_okay=False))
@click.argument("remote_name", required=False)
@click.option("--block-size", type=int, default=DEFAULT_BLOCK_SIZE, help="Tamaño de bloque en bytes")
def put_file(local_path, remote_name, block_size):
    """Subir un archivo (particiona en bloques y distribuye en DataNodes)"""
    try:
        if not remote_name:
            remote_name = os.path.basename(local_path)

        # 1) Obtener datanodes del NameNode
        dn_resp = requests.get(f"{NAMENODE_URL}/datanodes", headers=auth_headers())
        dn_resp.raise_for_status()
        datanodes = dn_resp.json().get("datanodes", [])
        if not datanodes:
            raise click.ClickException("No hay DataNodes registrados en NameNode")

        # 2) Subir bloques por round-robin
        file_size = os.path.getsize(local_path)
        total_blocks = math.ceil(file_size / block_size)
        blocks_meta = []

        click.echo(f"Subiendo '{local_path}' como '{remote_name}'")
        click.echo(f"Tamaño: {file_size} bytes, bloques: {total_blocks}, block_size={block_size}")

        with open(local_path, "rb") as f:
            for i in range(total_blocks):
                chunk = f.read(block_size)
                original_block_id = f"{remote_name}__{i}__{uuid.uuid4().hex}"
                dn = datanodes[i % len(datanodes)]
                safe_block_id = original_block_id.replace("/", "_")

                files = {"file": (safe_block_id, chunk, "application/octet-stream")}
                upload_url = f"{dn}/upload_block/{safe_block_id}"
                up = requests.post(upload_url, files=files, timeout=120)
                if up.status_code != 200:
                    raise click.ClickException(f"Error subiendo bloque {i} a {dn}: {up.text}")

                blocks_meta.append({"index": i, "block_id": safe_block_id, "datanode": dn})
                click.echo(f"  ✔ Bloque {i+1}/{total_blocks} -> {dn}")

        # 3) Registrar en NameNode
        reg = {
            "filename": remote_name,
            "size": file_size,
            "block_size": block_size,
            "blocks": blocks_meta
        }
        r = requests.post(f"{NAMENODE_URL}/register_file", json=reg, headers=auth_headers())
        r.raise_for_status()
        click.echo("Registro en NameNode OK")
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 400:
            click.echo(f"Error: File '{remote_name}' already exists")
        else:
            click.echo(f"Error uploading file: {e}")
    except Exception as e:
        click.echo(f"Error: {e}")

@cli.command("get")
@click.argument("remote_name")
@click.argument("output_path", type=click.Path(dir_okay=False))

def get_file(remote_name, output_path):
    """Descargar un archivo reconstruyéndolo desde sus bloques"""
    try:
        # URL encode the remote_name to handle paths with slashes
        encoded_name = quote(remote_name, safe='')
        r = requests.get(f"{NAMENODE_URL}/file/{encoded_name}", headers=auth_headers())
        
        if r.status_code != 200:
            raise click.ClickException(f"No encontrado en NameNode: {remote_name}")
        
        meta = r.json()
        blocks = meta.get("blocks", [])
        if not blocks:
            raise click.ClickException("No hay bloques registrados")

        # 2) Descargar en orden por index
        blocks = sorted(blocks, key=lambda b: b["index"])
        click.echo(f"Descargando '{remote_name}' -> '{output_path}' ({len(blocks)} bloques)")
        
        with open(output_path, "wb") as out:
            for i, b in enumerate(blocks):
                url = f"{b['datanode']}/block/{b['block_id']}"
                resp = requests.get(url, stream=True, timeout=120)
                if resp.status_code != 200:
                    raise click.ClickException(f"Error descargando bloque {i} desde {url}: {resp.text}")
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        out.write(chunk)
                click.echo(f"  ✔ Bloque {i+1}/{len(blocks)}")

        click.echo("Descarga completa")
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            click.echo(f"Error: File '{remote_name}' not found")
        else:
            click.echo(f"Error downloading file: {e}")
    except Exception as e:
        click.echo(f"Error: {e}")


@cli.command("mkdir")
@click.argument("dirname")
def mkdir(dirname):
    """Create a directory"""
    try:
        r = requests.post(f"{NAMENODE_URL}/mkdir/{dirname}", headers=auth_headers())
        r.raise_for_status()
        click.echo(r.json().get("msg"))
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 400:
            click.echo(f"Error: Directory '{dirname}' already exists")
        else:
            click.echo(f"Error creating directory: {e}")
    except Exception as e:
        click.echo(f"Error: {e}")

@cli.command("rmdir")
@click.argument("dirname")
def rmdir(dirname):
    """Remove a directory"""
    try:
        r = requests.delete(f"{NAMENODE_URL}/rmdir/{dirname}", headers=auth_headers())
        r.raise_for_status()
        click.echo(r.json().get("msg"))
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            click.echo(f"Error: Directory '{dirname}' not found")
        elif e.response.status_code == 400:
            click.echo(f"Error: Directory '{dirname}' is not empty")
        else:
            click.echo(f"Error removing directory: {e}")
    except Exception as e:
        click.echo(f"Error: {e}")

@cli.command("rm")
@click.argument("filename")
def rm(filename):
    """Remove a file"""
    try:
        r = requests.delete(f"{NAMENODE_URL}/rm/{filename}", headers=auth_headers())
        r.raise_for_status()
        click.echo(r.json().get("msg"))
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            click.echo(f"Error: File '{filename}' not found")
        else:
            click.echo(f"Error removing file: {e}")
    except Exception as e:
        click.echo(f"Error: {e}")

@cli.command("ls")
@click.argument("path", required=False, default="/")
def list_files(path):
    """List files and directories in a path"""
    try:
        r = requests.get(f"{NAMENODE_URL}/ls?path={path}", headers=auth_headers())
        r.raise_for_status()
        data = r.json()
        
        click.echo(f"Contents of {data['path']}:")
        for item in data.get("items", []):
            if item["type"] == "directory":
                click.echo(f"d {item['name']}/")
            else:
                click.echo(f"- {item['name']} ({item['size']} bytes, block_size={item['block_size']})")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            click.echo(f"Error: Path '{path}' not found")
        else:
            click.echo(f"Error listing directory: {e}")
    except Exception as e:
        click.echo(f"Error: {e}")

if __name__ == "__main__":
    cli()
