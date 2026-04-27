import pytz
from datetime import datetime

# Configuración de Timezone centralizada
COLOMBIA_TZ = pytz.timezone('America/Bogota')

def now_colombia() -> datetime:
    """Retorna la hora actual en Colombia con información de zona horaria."""
    return datetime.now(COLOMBIA_TZ)
