import asyncio
import time
import logging
import json
from datetime import timedelta
from typing import Optional, Dict, List
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from directus_client import (
    authenticate_websocket_token,
    update_client_last_ping,
    check_directus_connection,
    _directus_request,
    validate_token,
    get_async_client
)
from services import (
    resolve_destinations,
    deliver_pending_notifications,
    save_notification_log,
    save_pending_notifications
)
from security import security
from utils import now_colombia

logger = logging.getLogger(__name__)

# Gestión de clientes segura para concurrencia
connected_clients: Dict[str, List[dict]] = {}
clients_lock = asyncio.Lock()

router = APIRouter()

async def marcar_clientes_inactivos():
    """Tarea periódica para limpiar clientes desconectados en Directus."""
    try:
        # Obtener clientes que no han reportado ping en los últimos 2 minutos
        cutoff = (now_colombia() - timedelta(seconds=120)).isoformat()
        
        async with clients_lock:
            # Solo buscar clientes que Directus cree que están activos
            all_active = await _directus_request("/items/core_notifier_clients", params={
                "filter": json.dumps({
                    "is_active": {"_eq": True},
                    "last_ping": {"_lt": cutoff}
                }),
                "fields": "id,code"
            })

            client_dx = get_async_client()
            for c in all_active:
                code = str(c["code"])
                # Si no está en nuestro mapa local de WebSockets, marcar como inactivo
                if code not in connected_clients:
                    await client_dx.patch(f"/items/core_notifier_clients/{c['id']}", json={"is_active": False})
                    logger.info(f"[-] Cliente {code} marcado inactivo por inactividad prolongada")
    except Exception as e:
        logger.error(f"Error en marcar_clientes_inactivos: {e}")

@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, token: Optional[str] = Query(None), version: str = Query("1.0.0")):
    client_ip = websocket.client.host if websocket.client else "unknown"

    # 1. Seguridad inicial
    if security.is_ip_blocked(client_ip):
        await websocket.close(code=4003, reason="IP bloqueada")
        return
    
    if not security.check_rate_limit(client_ip):
        await websocket.close(code=4029, reason="Rate limit excedido")
        return

    # 2. Autenticación
    auth_header = websocket.headers.get("authorization", "")
    resolved_token = auth_header[7:] if auth_header.startswith("Bearer ") else token
    
    if not resolved_token:
        await websocket.close(code=4001, reason="Token requerido")
        return

    client_record = await authenticate_websocket_token(resolved_token, version=version)
    if not client_record:
        security.record_failed_attempt(client_ip)
        await websocket.close(code=4001, reason="Token inválido")
        return

    client_code = str(client_record.get("code", "unknown"))
    client_id = str(client_record["id"])
    
    await websocket.accept()
    security.record_successful_connection(client_ip)

    # 3. Registro de conexión (Thread-safe)
    async with clients_lock:
        if client_code not in connected_clients:
            connected_clients[client_code] = []
        
        # Evitar duplicados del mismo socket
        connected_clients[client_code].append({
            "ws": websocket,
            "id": client_id,
            "connected_at": now_colombia().isoformat(),
            "last_sync": time.time()
        })

    logger.info(f"[✓] WS Conectado: {client_code} (IP: {client_ip})")
    await update_client_last_ping(client_id)

    # 4. Entregar pendientes
    try:
        await deliver_pending_notifications(client_code, client_id, websocket)
    except Exception as e:
        logger.error(f"Error entregando pendientes a {client_code}: {e}")

    # 5. Loop de vida
    try:
        while True:
            # Esperar ping del cliente (timeout de 60s)
            msg = await asyncio.wait_for(websocket.receive_text(), timeout=60)
            if msg.strip().lower() == "ping":
                await websocket.send_text("pong")
                
                # Sincronizar ping en DB cada 5 minutos para ahorrar recursos
                async with clients_lock:
                    for conn in connected_clients.get(client_code, []):
                        if conn["ws"] == websocket:
                            if (time.time() - conn["last_sync"]) > 300:
                                await update_client_last_ping(client_id)
                                conn["last_sync"] = time.time()
                            break
    except (WebSocketDisconnect, asyncio.TimeoutError):
        logger.info(f"[-] WS Desconectado: {client_code}")
    except Exception as e:
        logger.warning(f"[!] Error WS {client_code}: {e}")
    finally:
        async with clients_lock:
            if client_code in connected_clients:
                connected_clients[client_code] = [c for c in connected_clients[client_code] if c["ws"] != websocket]
                if not connected_clients[client_code]:
                    del connected_clients[client_code]
        
        await update_client_last_ping(client_id)
        # Marcar inactivo en DB al desconectar el último socket de este código
        if client_code not in connected_clients:
            try:
                await get_async_client().patch(f"/items/core_notifier_clients/{client_id}", json={"is_active": False})
            except: pass

@router.get("/health")
async def health_check():
    directus_ok = await check_directus_connection()
    return {
        "status": "ok" if directus_ok else "degraded",
        "clients_connected": sum(len(v) for v in connected_clients.values()),
        "timestamp": now_colombia().isoformat()
    }

@router.get("/clients")
async def get_clients(token: Optional[str] = Query(None), authorization: Optional[str] = Header(None)):
    # Permitir token por Header o por URL (?token=...)
    resolved_token = ""
    if authorization and authorization.startswith("Bearer "):
        resolved_token = authorization[7:]
    elif token:
        resolved_token = token

    if not resolved_token:
        raise HTTPException(status_code=401, detail="Token requerido (via Header o ?token=)")

    user = await validate_token(resolved_token)
    if not user:
        raise HTTPException(status_code=401, detail="Token inválido")

    """Devuelve la lista de equipos conectados (solo para usuarios autenticados)."""
    async with clients_lock:
        return {
            "total": len(connected_clients),
            "clients": list(connected_clients.keys())
        }


@router.post("/notify")
async def enviar_notificacion(request: Request, authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Token requerido")

    user = await validate_token(authorization[7:])
    if not user:
        raise HTTPException(status_code=401, detail="Token inválido")

    try:
        data = await request.json()
    except:
        raise HTTPException(status_code=400, detail="JSON inválido")

    # Resolución de destinos
    destinatarios = data.get("destinatarios", [])
    excluir = data.get("excluir", [])
    target_ids, raw_dest, id_map = await resolve_destinations(destinatarios, excluir)

    if not target_ids:
        return JSONResponse(status_code=400, content={"message": "Sin destinatarios válidos"})

    # Enviar a conectados
    enviados_ids = set()
    async with clients_lock:
        for code, conns in connected_clients.items():
            for conn in conns:
                cid = conn["id"]
                if cid in target_ids:
                    try:
                        await conn["ws"].send_json({
                            "titulo": data.get("titulo", "Notificación"),
                            "mensaje": data.get("mensaje", ""),
                            "tipo": data.get("tipo", "info"),
                            "duracion_seg": data.get("duracion_seg", 15),
                            "persistente": data.get("persistente", False),
                            "clickeable": data.get("clickeable", True),
                            "ruta_accion": data.get("ruta_accion")
                        })
                        enviados_ids.add(cid)
                    except:
                        logger.error(f"Error enviando a socket de {code}")

    # Procesar pendientes
    pendientes_ids = list(set(target_ids) - enviados_ids)
    
    # Guardar LOG
    notif_id = await save_notification_log(
        titulo=data.get("titulo", ""),
        mensaje=data.get("mensaje", ""),
        tipo=data.get("tipo", "info"),
        remitente=f"{user.get('first_name', '')} {user.get('last_name', '')}".strip() or user.get("email"),
        ip_origen=request.client.host if request.client else "unknown",
        destinos_raw=raw_dest,
        destinos_reales=target_ids,
        enviados=len(enviados_ids),
        pendientes=len(pendientes_ids),
        duracion_seg=data.get("duracion_seg", 15),
        persistente=data.get("persistente", False)
    )

    # Guardar pendientes en batch
    if pendientes_ids and notif_id:
        await save_pending_notifications(pendientes_ids, notif_id)

    return {
        "status": "ok",
        "enviados": len(enviados_ids),
        "pendientes": len(pendientes_ids),
        "detalle": {
            "enviados": [id_map.get(i, i) for i in enviados_ids],
            "pendientes": [id_map.get(i, i) for i in pendientes_ids]
        }
    }
