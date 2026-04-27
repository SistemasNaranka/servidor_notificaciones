import os
import pytz
from dotenv import load_dotenv

load_dotenv()

DIRECTUS_URL = os.getenv("DIRECTUS_URL", "http://192.168.19.245:8085")
DIRECTUS_TOKEN = os.getenv("DIRECTUS_TOKEN", "")
SERVER_HOST = os.getenv("SERVER_HOST", "0.0.0.0")
SERVER_PORT = int(os.getenv("SERVER_PORT", "5050"))

# Configuración adicional para entorno LAN
DIRECTUS_VERIFY_SSL = os.getenv("DIRECTUS_VERIFY_SSL", "False").lower() == "true"
COLOMBIA_TZ = pytz.timezone('America/Bogota')
