from fastapi import FastAPI, UploadFile, HTTPException
from fastapi.responses import FileResponse, JSONResponse
import os

app = FastAPI()
STORAGE_DIR = "/data"
os.makedirs(STORAGE_DIR, exist_ok=True)

@app.get("/")
def health():
    return {"status": "datanode alive"}

@app.post("/upload_block/{block_id}")
async def upload_block(block_id: str, file: UploadFile):
    file_path = os.path.join(STORAGE_DIR, block_id)
    try:
        with open(file_path, "wb") as f:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)
    finally:
        await file.close()
    return {"msg": f"block {block_id} stored"}

@app.get("/block/{block_id}")
def get_block(block_id: str):
    file_path = os.path.join(STORAGE_DIR, block_id)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Block not found")
    return FileResponse(file_path, media_type="application/octet-stream", filename=block_id)

@app.get("/exists/{block_id}")
def exists(block_id: str):
    return {"exists": os.path.exists(os.path.join(STORAGE_DIR, block_id))}
