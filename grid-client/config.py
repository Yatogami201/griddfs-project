import os
from dotenv import load_dotenv

load_dotenv()

# Configuración por environment variables o archivo .env
NAMENODE_URL = os.getenv("NAMENODE_URL", "http://localhost:5000")
BLOCK_SIZE_MB = int(os.getenv("BLOCK_SIZE_MB", "4"))
DEFAULT_BLOCK_SIZE = BLOCK_SIZE_MB * 1024 * 1024
TOKEN_FILE = os.getenv("TOKEN_FILE", ".griddfs_token")