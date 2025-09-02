import os
import math
import uuid
import click
import requests

NAMENODE_URL = os.getenv("NAMENODE_URL", "http://namenode:8000")
DEFAULT_BLOCK_SIZE_MB = int(os.getenv("BLOCK_SIZE_MB", "4"))  # ajústalo (p.ej. 64)
DEFAULT_BLOCK_SIZE = DEFAULT_BLOCK_SIZE_MB * 1024 * 1024

@click.group()
def cli():
    """GridDFS CLI"""
    pass

@cli.command()
def ping():
    """Probar conexión al NameNode"""
    r = requests.get(f"{NAMENODE_URL}/")
    click.echo(r.json())

@cli.command("ls")
def list_files():
    """Listar archivos registrados en el NameNode"""
    r = requests.get(f"{NAMENODE_URL}/ls")
    r.raise_for_status()
    data = r.json()
    for f in data.get("files", []):
        click.echo(f"- {f['filename']} ({f['size']} bytes, block_size={f['block_size']})")

@cli.command("put")
@click.argument("local_path", type=click.Path(exists=True, dir_okay=False))
@click.argument("remote_name", required=False)
@click.option("--block-size", type=int, default=DEFAULT_BLOCK_SIZE, help="Tamaño de bloque en bytes")
def put_file(local_path, remote_name, block_size):
    """Subir un archivo (particiona en bloques y distribuye en DataNodes)"""
    if not remote_name:
        remote_name = os.path.basename(local_path)

    # 1) Obtener datanodes del NameNode
    dn_resp = requests.get(f"{NAMENODE_URL}/datanodes")
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
            block_id = f"{remote_name}__{i}__{uuid.uuid4().hex}"
            dn = datanodes[i % len(datanodes)]

            files = {"file": (block_id, chunk, "application/octet-stream")}
            upload_url = f"{dn}/upload_block/{block_id}"
            up = requests.post(upload_url, files=files, timeout=120)
            if up.status_code != 200:
                raise click.ClickException(f"Error subiendo bloque {i} a {dn}: {up.text}")

            blocks_meta.append({"index": i, "block_id": block_id, "datanode": dn})
            click.echo(f"  ✔ Bloque {i+1}/{total_blocks} -> {dn}")

    # 3) Registrar en NameNode
    reg = {
        "filename": remote_name,
        "size": file_size,
        "block_size": block_size,
        "blocks": blocks_meta
    }
    r = requests.post(f"{NAMENODE_URL}/register_file", json=reg)
    r.raise_for_status()
    click.echo("Registro en NameNode OK")

@cli.command("get")
@click.argument("remote_name")
@click.argument("output_path", type=click.Path(dir_okay=False))
def get_file(remote_name, output_path):
    """Descargar un archivo reconstruyéndolo desde sus bloques"""
    # 1) Preguntar metadatos al NameNode
    r = requests.get(f"{NAMENODE_URL}/file/{remote_name}")
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

    click.echo("Descarga completa ✅")

if __name__ == "__main__":
    cli()
