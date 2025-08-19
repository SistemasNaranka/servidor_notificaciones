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

    # Si ya existe un cliente con ese user_id, cierra su conexión anterior
    if user_id in connected_clients:
        for client in connected_clients[user_id]:
            try:
                await client["ws"].close()
            except:
                pass
        connected_clients[user_id].clear()

    # Guarda la nueva conexión (solo una por user_id)
    connected_clients[user_id] = [{
        "ws": websocket,
        "nombre": nombre
    }]

    logger.info(f"[✓] Cliente conectado: {user_id} - {nombre}")

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        logger.info(f"[-] Cliente desconectado: {user_id} - {nombre}")
        connected_clients.pop(user_id, None)


@app.get("/online")
def get_connected_users():
    online = [
        f"{user_id}: {info[0].get('nombre', '')}"  # Solo muestra el nombre del primer cliente en la lista
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
                encontrados.append((destinatario, client))  # <-- FIX

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
