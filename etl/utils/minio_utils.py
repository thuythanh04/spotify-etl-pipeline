from minio import Minio
from config import MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ENDPOINT

def init_minio_client() -> Minio:
    """Initialize and return a Minio client."""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )