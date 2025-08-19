# archivo: notification_server.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import JSONResponse
from typing import Dict, List
from fastapi import Request
import uvicorn
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


app = FastAPI()
connected_clients: Dict[str, List[Dict]] = {}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, user_id: str = Query(...), nombre: str = Query(default="")):
    await websocket.accept()
    if user_id not in connected_clients:
        connected_clients[user_id] = []
    # Agrega la nueva conexión a la lista
    connected_clients[user_id].append({
        "ws": websocket,
        "nombre": nombre
    })
    logger.info(f"[✓] Cliente conectado: {user_id} - {nombre}")

    try:
        while True:
            await websocket.receive_text()  # mantener viva la conexión
    except WebSocketDisconnect:
        logger.info(f"[-] Cliente desconectado: {user_id} - {nombre}")
        # Eliminar la conexión de la lista
        connected_clients[user_id] = [client for client in connected_clients[user_id] if client["ws"] != websocket]
        # Si la lista está vacía, eliminar el user_id del diccionario
        if not connected_clients[user_id]:
            del connected_clients[user_id]

@app.get("/online")
def get_connected_users():
    online = [
        f"{user_id}: {info.get('nombre', '')}" 
        for user_id, info in connected_clients.items()
    ]
    total = sum(len(info) for info in connected_clients.values())  # Total de conexiones
    return JSONResponse(content={
        "total_conectados": total,
        "online_users": online
    })

@app.post("/notify")
async def enviar_notificacion(request: Request):
    data = await request.json()
    destinatarios = data.get("destinatarios")

    if isinstance(destinatarios, str):
        destinatarios = [destinatarios]

    enviados = []
    no_conectados = []

    for destinatario in destinatarios:
        encontrados = []

        # Buscar por ID directamente
        if destinatario in connected_clients:
            for client in connected_clients[destinatario]:
                encontrados.append((destinatario, client))

        # Buscar por nombre
        for uid, info in connected_clients.items():
            for client in info:
                if client.get("nombre") == destinatario:
                    encontrados.append((uid, client))

        if encontrados:
            for uid, client in encontrados:
                try:
                    await client["ws"].send_json(data)
                    enviados.append(f"{uid}: {client.get('nombre', '')}")
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
