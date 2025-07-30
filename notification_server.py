# archivo: notification_server.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import JSONResponse
from typing import Dict
from fastapi import Request
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


app = FastAPI()
connected_clients: Dict[str, Dict] = {}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, user_id: str = Query(...), nombre: str = Query(default="")):
    await websocket.accept()
    connected_clients[user_id] = {
        "ws": websocket,
        "nombre": nombre
    }
    logger.info(f"[✓] Cliente conectado: {user_id} - {nombre}")

    try:
        while True:
            await websocket.receive_text()  # mantener viva la conexión
    except WebSocketDisconnect:
        logger.info(f"[-] Cliente desconectado: {user_id}")
        connected_clients.pop(user_id, None)


@app.get("/online")
def get_connected_users():
    online = [
        f"{user_id}: {info.get('nombre', '')}" 
        for user_id, info in connected_clients.items()
    ]
    return JSONResponse(content={"online_users": online})


@app.post("/notify")
async def enviar_notificacion(request: Request):
    data = await request.json()
    destinatarios = data.get("destinatarios")

    if isinstance(destinatarios, str):
        destinatarios = [destinatarios]

    enviados = []
    no_conectados = []
    ya_enviados = set()

    for destinatario in destinatarios:
        encontrados = []

        # Buscar por ID directamente
        if destinatario in connected_clients:
            encontrados.append((destinatario, connected_clients[destinatario]))

        # Buscar por nombre
        for uid, info in connected_clients.items():
            if info.get("nombre") == destinatario and uid not in ya_enviados:
                encontrados.append((uid, info))

        if encontrados:
            for uid, info in encontrados:
                if uid not in ya_enviados:
                    try:
                        await info["ws"].send_json(data)
                        enviados.append(f"{uid}: {info.get('nombre', '')}")
                        ya_enviados.add(uid)
                    except Exception as e:
                        logger.error(f"Error enviando a {uid}: {e}")
                        no_conectados.append(destinatario)
        else:
            no_conectados.append(destinatario)

    return {
        "status": "parcial" if no_conectados else "enviado",
        "enviados": enviados,
        "no_conectados": no_conectados
    }


# @app.post("/notify/email")
# async def enviar_email(...):
#     pass

# @app.post("/notify/telegram")
# async def enviar_telegram(...):
#     pass