import logging
import asyncio
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from routes import router, marcar_clientes_inactivos, procesar_notificaciones_pendientes_online
from config import SERVER_HOST, SERVER_PORT

# Configuración de logging profesional
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("main")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestión del ciclo de vida de la aplicación."""
    logger.info("Iniciando servidor de notificaciones...")
    
    # Tarea periódica: Gestión de inactivos y liberación de programadas
    async def tarea_periodica():
        logger.info("[Background] Tarea periódica iniciada")
        while True:
            try:
                # 1. Limpiar clientes que no reportan ping
                await marcar_clientes_inactivos()
                # 2. Procesar notificaciones programadas
                await procesar_notificaciones_pendientes_online()
            except Exception as e:
                logger.error(f"[Background] Error en ciclo de tarea: {e}")
            
            await asyncio.sleep(60) # Cada minuto es suficiente y reduce I/O

    bg_task = asyncio.create_task(tarea_periodica())
    
    yield
    
    logger.info("Cerrando servidor...")
    bg_task.cancel()
    try:
        await bg_task
    except (asyncio.CancelledError, Exception):
        pass


app = FastAPI(
    title="Servidor de Notificaciones",
    description="Backend asíncrono para gestión de notificaciones en tiempo real",
    version="2.0.0",
    lifespan=lifespan
)

# Configuración de CORS segura para la red LAN
app.add_middleware(
    CORSMiddleware,
    # Permite orígenes que coincidan con la red 192.168.19.x
    allow_origin_regex=r"http://192\.168\.19\..*",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

if __name__ == "__main__":
    # Reload=False para producción por estabilidad
    uvicorn.run("main:app", host=SERVER_HOST, port=SERVER_PORT, reload=False)
