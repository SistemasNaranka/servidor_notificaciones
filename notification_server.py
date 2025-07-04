# archivo: notification_server.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import JSONResponse
from typing import Dict
import uvicorn
from fastapi import Request
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


app = FastAPI()
connected_clients: Dict[str, WebSocket] = {}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, user_id: str = Query(...)):
    await websocket.accept()
    connected_clients[user_id] = websocket
    logger.info(f"[✓] Cliente conectado: {user_id}")

    try:
        while True:
            await websocket.receive_text()  # mantener viva la conexión
    except WebSocketDisconnect:
        logger.info(f"[-] Cliente desconectado: {user_id}")
        connected_clients.pop(user_id, None)


@app.get("/online")
def get_connected_users():
    return JSONResponse(content={"online_users": list(connected_clients.keys())})

@app.post("/notify")
async def enviar_notificacion(request: Request):
    data = await request.json()
    user_ids = data.get("destinatarios")

    if isinstance(user_ids, str):
        user_ids = [user_ids]

    enviados = []
    no_conectados = []

    for uid in user_ids:
        if uid in connected_clients:
            await connected_clients[uid].send_json(data)
            enviados.append(uid)
        else:
            no_conectados.append(uid)

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