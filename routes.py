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

            client_dx = await get_async_client()
            for c in all_active:
                code = str(c["code"])
                # Si no está en nuestro mapa local de WebSockets, marcar como inactivo
                if code not in connected_clients:
                    await client_dx.patch(f"/items/core_notifier_clients/{c['id']}", json={"is_active": False})
                    logger.info(f"[-] Cliente {code} marcado inactivo por inactividad prolongada")
    except Exception as e:
        logger.error(f"Error en marcar_clientes_inactivos: {e}")

@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, version: str = "1.0.0", device: str = "unknown"):
    """Maneja las conexiones WebSocket de los clientes."""
    client_ip = websocket.client.host if websocket.client else "unknown"

    # 1. Seguridad inicial (Pre-accept)
    if security.is_ip_blocked(client_ip):
        await websocket.close(code=4003, reason="IP bloqueada")
        return
    
    if not security.check_rate_limit(client_ip):
        await websocket.close(code=4029, reason="Rate limit excedido")
        return

    # 2. Autenticación (Pre-accept)
    auth_header = websocket.headers.get("authorization", "")
    resolved_token = auth_header[7:] if auth_header.startswith("Bearer ") else None
    
    if not resolved_token:
        await websocket.close(code=4001, reason="Token requerido en cabecera Authorization")
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
        
        connected_clients[client_code].append({
            "ws": websocket,
            "id": client_id,
            "name": client_record.get("name", "Sin Nombre"),
            "ip": client_ip,
            "version": version,
            "device": device,
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
            msg_raw = await asyncio.wait_for(websocket.receive_text(), timeout=60)
            
            # Manejo de Pings
            if msg_raw.strip().lower() == "ping":
                await websocket.send_text("pong")
                async with clients_lock:
                    for conn in connected_clients.get(client_code, []):
                        if conn["ws"] == websocket:
                            if (time.time() - conn["last_sync"]) > 300:
                                await update_client_last_ping(client_id)
                                conn["last_sync"] = time.time()
                            break
                continue

            # Manejo de ACKs y otros mensajes JSON
            try:
                data = json.loads(msg_raw)
                if data.get("type") == "ack":
                    pending_id = data.get("pending_id")
                    if pending_id:
                        await (await get_async_client()).patch(
                            f"/items/core_notifications_pending/{pending_id}", 
                            json={"is_delivered": True}
                        )
                        logger.info(f"[ACK] Notificación {pending_id} confirmada por {client_code}")
            except json.JSONDecodeError:
                pass
            except Exception as e:
                logger.error(f"Error procesando mensaje de cliente {client_code}: {e}")

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
        if client_code not in connected_clients:
            try:
                await (await get_async_client()).patch(f"/items/core_notifier_clients/{client_id}", json={"is_active": False})
            except Exception as e:
                logger.debug(f"Error marcando inactivo al desconectar: {e}")


@router.get("/health")
async def health_check():
    directus_ok = await check_directus_connection()
    return {
        "status": "ok" if directus_ok else "degraded",
        "clients_connected": sum(len(v) for v in connected_clients.values()),
        "timestamp": now_colombia().isoformat()
    }

@router.get("/notify/docs")
async def get_documentation(authorization: Optional[str] = Header(None)):
    """Devuelve la documentación técnica del endpoint de notificaciones (Requiere Token)."""
    resolved_token = authorization[7:] if authorization and authorization.startswith("Bearer ") else None

    if not resolved_token:
        raise HTTPException(status_code=401, detail="Token requerido en cabecera Authorization")

    user = await validate_token(resolved_token)
    if not user:
        raise HTTPException(status_code=401, detail="Token inválido")


    return {
        "endpoint": "/notify",
        "method": "POST",
        "auth": "Bearer Token requerido en cabecera Authorization",
        "description": "Envía notificaciones en tiempo real a los clientes conectados o las guarda como pendientes.",
        "parameters": {
            "destinatarios": {
                "type": "list[str]",
                "description": "Lista de códigos de cliente, áreas ('area:Nombre') o grupos ('grupo:Nombre'). Use 'todos' para difusión masiva.",
                "required": True
            },
            "titulo": {
                "type": "string",
                "description": "Título de la notificación.",
                "default": "Notificación"
            },
            "mensaje": {
                "type": "string",
                "description": "Contenido del mensaje.",
                "default": ""
            },
            "tipo": {
                "type": "string",
                "description": "Estilo visual: 'info', 'success', 'warning', 'error'.",
                "default": "info"
            },
            "duracion_seg": {
                "type": "int",
                "description": "Tiempo de visualización en segundos.",
                "default": 15
            },
            "persistente": {
                "type": "boolean",
                "description": "Si es true, la notificación no se cerrará automáticamente.",
                "default": False
            },
            "clickeable": {
                "type": "boolean",
                "description": "Si es true, el usuario puede interactuar con la notificación.",
                "default": True
            },
            "mostrar_boton_cerrar": {
                "type": "boolean",
                "description": "Muestra u oculta el botón 'X' de cierre.",
                "default": True
            },
            "pausar_al_hover": {
                "type": "boolean",
                "description": "Detiene el temporizador de cierre cuando el mouse está sobre la notificación.",
                "default": True
            },
            "excluir": {
                "type": "list[str]",
                "description": "Lista de códigos de cliente a excluir del envío.",
                "default": []
            },
            "scheduled_date": {
                "type": "string",
                "description": "Fecha y hora de envío programado (ISO 8601). Ej: 2026-04-30T15:00:00",
                "default": "now"
            }
        },
        "example_payload": {
            "destinatarios": ["4444", "area:Contabilidad"],
            "titulo": "Prueba de Notificación",
            "mensaje": "Este es un mensaje de prueba.",
            "tipo": "success",
            "duracion_seg": 20,
            "mostrar_boton_cerrar": True,
            "pausar_al_hover": True
        }
    }


@router.get("/clients")
async def get_clients(authorization: Optional[str] = Header(None)):
    """Devuelve SOLO los clientes que están conectados actualmente."""
    resolved_token = authorization[7:] if authorization and authorization.startswith("Bearer ") else None
    
    if not resolved_token or not await validate_token(resolved_token):
        raise HTTPException(status_code=401, detail="No autorizado")

    try:
        active_list = []
        async with clients_lock:
            for code, conns in connected_clients.items():
                for c in conns:
                    active_list.append({
                        "id": c["id"],
                        "code": code,
                        "name": c.get("name", "Sin Nombre")
                    })

        return {
            "total_online": len(active_list),
            "clients": active_list
        }
    except Exception as e:
        logger.error(f"Error en get_clients: {e}")
        raise HTTPException(status_code=500, detail="Error interno")

@router.get("/clients/detail")
async def get_clients_detail(authorization: Optional[str] = Header(None)):
    """Devuelve la lista completa de la DB con detalles técnicos de conexión."""
    resolved_token = authorization[7:] if authorization and authorization.startswith("Bearer ") else None
    if not resolved_token or not await validate_token(resolved_token):
        raise HTTPException(status_code=401, detail="No autorizado")


    try:
        # 1. Obtener todos los de la DB
        all_db = await _directus_request("/items/core_notifier_clients", params={"fields": "*"})
        
        # 2. Mapear conexiones activas para cruzar datos
        active_map = {}
        async with clients_lock:
            for code, conns in connected_clients.items():
                for c in conns:
                    active_map[str(c["id"])] = c

        # 3. Construir reporte completo
        report = []
        for db_c in all_db:
            cid = str(db_c["id"])
            is_online = cid in active_map
            
            item = {
                "id": db_c["id"],
                "code": db_c["code"],
                "name": db_c["name"],
                "is_online": is_online,
                # Datos técnicos (solo si está online, si no, lo que haya en DB)
                "ip": active_map[cid]["ip"] if is_online else db_c.get("last_ip"),
                "version": active_map[cid]["version"] if is_online else db_c.get("version"),
                "device": active_map[cid]["device"] if is_online else "Offline",
                "connected_at": active_map[cid]["connected_at"] if is_online else None,
                "last_ping": db_c.get("last_ping")
            }
            report.append(item)

        return report
    except Exception as e:
        logger.error(f"Error en get_clients_detail: {e}")
        raise HTTPException(status_code=500, detail="Error al generar detalle")


from schemas import NotificationRequest

@router.post("/notify")
async def enviar_notificacion(payload: NotificationRequest, request: Request, authorization: Optional[str] = Header(None)):
    """Envía notificaciones con validación Pydantic y soporte de ACK."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Token requerido")

    user = await validate_token(authorization[7:])
    if not user:
        raise HTTPException(status_code=401, detail="Token inválido")

    # 1. Validación de fecha programada
    now = now_colombia()
    is_scheduled = False
    if payload.scheduled_date:
        try:
            from datetime import datetime
            dt_str = payload.scheduled_date.replace('Z', '+00:00')
            scheduled_dt = datetime.fromisoformat(dt_str)
            if scheduled_dt.tzinfo is None:
                scheduled_dt = scheduled_dt.replace(tzinfo=now.tzinfo)
            if scheduled_dt > now:
                is_scheduled = True
        except Exception:
            raise HTTPException(status_code=400, detail="Formato de scheduled_date inválido")

    # 2. Resolución de destinos
    target_ids, raw_dest, id_map = await resolve_destinations(payload.destinatarios, payload.excluir)
    if not target_ids:
        return JSONResponse(status_code=400, content={"message": "Sin destinatarios válidos"})

    # 3. Guardar log principal
    notif_id = await save_notification_log(
        titulo=payload.titulo,
        mensaje=payload.mensaje,
        tipo=payload.tipo,
        remitente=f"{user.get('first_name', '')} {user.get('last_name', '')}".strip() or user.get("email"),
        ip_origen=request.client.host if request.client else "unknown",
        destinos_raw=raw_dest,
        destinos_reales=target_ids,
        enviados=0, # Se actualizará si es necesario, o se deja como referencia
        pendientes=len(target_ids),
        duracion_seg=payload.duracion_seg,
        persistente=payload.persistente,
        mostrar_boton_cerrar=payload.mostrar_boton_cerrar,
        pausar_al_hover=payload.pausar_al_hover,
        scheduled_date=payload.scheduled_date if is_scheduled else None,
        ruta_accion=payload.ruta_accion
    )

    if not notif_id:
        raise HTTPException(status_code=500, detail="Error al guardar notificación")

    # 4. Crear registros pendientes en DB (Obtenemos los IDs para el ACK)
    # Modificamos save_pending_notifications para que devuelva los IDs creados
    created_pendings = await save_pending_notifications(
        target_ids, 
        notif_id, 
        scheduled_date=payload.scheduled_date if is_scheduled else None
    )
    
    # Mapear client_id -> pending_id
    pending_map = {str(p["client_id"]): str(p["id"]) for p in created_pendings}

    # 5. Intentar envío por WebSocket si no es diferido
    enviados = []
    online_ids = set()
    enviados_count = 0
    
    if not is_scheduled:
        async with clients_lock:
            for code, conns in connected_clients.items():
                for conn in conns:
                    cid = str(conn["id"])
                    if cid in pending_map:
                        try:
                            await conn["ws"].send_json({
                                "pending_id": pending_map[cid],
                                "titulo": payload.titulo,
                                "mensaje": payload.mensaje,
                                "tipo": payload.tipo,
                                "duracion_seg": payload.duracion_seg,
                                "persistente": payload.persistente,
                                "clickeable": payload.clickeable,
                                "mostrar_boton_cerrar": payload.mostrar_boton_cerrar,
                                "pausar_al_hover": payload.pausar_al_hover,
                                "ruta_accion": payload.ruta_accion
                            })
                            enviados_count += 1
                            online_ids.add(cid)
                            enviados.append(id_map.get(cid, code))
                        except:
                            logger.error(f"Error enviando WS a {code}")

    # 6. Calcular pendientes (offline o programados)
    pendientes = []
    for cid in target_ids:
        if cid not in online_ids:
            pendientes.append(id_map.get(cid, "Desconocido"))

    # 7. Determinar status humano
    status_msg = "completado"
    if is_scheduled:
        status_msg = "programado"
    elif enviados_count == 0:
        status_msg = "pendiente"
    elif len(pendientes) > 0:
        status_msg = "parcial"

    return {
        "status": status_msg,
        "notif_id": notif_id,
        "enviados": enviados,
        "pendientes": pendientes,
        "total": len(target_ids)
    }


async def procesar_notificaciones_pendientes_online():
    """Busca notificaciones cuya fecha llegó y el cliente está conectado."""
    try:
        now = now_colombia()
        # 1. Obtener registros pendientes que ya deben liberarse
        pending = await _directus_request("/items/core_notifications_pending", {
            "filter": json.dumps({
                "is_delivered": {"_eq": False},
                "scheduled_date": {"_lte": now.isoformat()}
            }),
            "fields": "id,client_id,notification_id"
        })

        if not pending: return

        # 2. Mapear clientes conectados para acceso rápido
        async with clients_lock:
            online_map = {}
            for code, conns in connected_clients.items():
                for conn in conns:
                    online_map[str(conn["id"])] = conn["ws"]

        # 3. Procesar envíos de forma optimizada
        client_dx = await get_async_client()
        
        # Agrupar por ID de notificación para evitar N+1 queries
        unique_notif_ids = list(set(str(p["notification_id"]) for p in pending if p.get("notification_id")))
        if not unique_notif_ids: return

        notifs_res = await _directus_request("/items/core_notifications", {
            "filter": json.dumps({"id": {"_in": unique_notif_ids}}),
            "fields": "*"
        })
        notif_data_map = {str(n["id"]): n for n in notifs_res}

        for p in pending:
            cid = str(p["client_id"])
            if cid in online_map:
                try:
                    n_id = str(p["notification_id"])
                    n_data = notif_data_map.get(n_id)
                    
                    if n_data:
                        await online_map[cid].send_json({
                            "titulo": n_data.get("title", "Notificación"),
                            "mensaje": n_data.get("message", ""),
                            "tipo": n_data.get("notification_type", "info"),
                            "duracion_seg": n_data.get("duration_seconds", 15),
                            "persistente": n_data.get("is_persistent", False),
                            "mostrar_boton_cerrar": n_data.get("show_close_button", True),
                            "pausar_al_hover": n_data.get("pause_on_hover", True),
                            "ruta_accion": n_data.get("action_route"),
                            "programada": True
                        })
                        
                        # Marcar como entregada de forma individual (para asegurar trazabilidad por registro)
                        await client_dx.patch(f"/items/core_notifications_pending/{p['id']}", json={"is_delivered": True})
                        logger.info(f"[✓] Notificación programada liberada para cliente ID {cid}")
                except Exception as e:
                    logger.error(f"Error liberando programada {p['id']}: {e}")


    except Exception as e:
        logger.error(f"Error en procesar_notificaciones_pendientes_online: {e}")
