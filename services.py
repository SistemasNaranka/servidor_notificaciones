import json
import logging
import asyncio
from typing import List, Set, Optional, Tuple, Dict
from directus_client import get_async_client, _directus_request
from utils import now_colombia
from datetime import timedelta

logger = logging.getLogger(__name__)

async def resolve_destinations(destinatarios: list, excluir: list = None) -> tuple:
    """
    Traduce destinatarios a IDs de clientes específicos con filtros inteligentes (Async).
    Retorna (list_de_ids, lista_raw_para_logs, id_to_code)
    """
    client_ids = set()
    id_to_code: Dict[str, str] = {}
    destinos_raw = [str(d).strip() for d in destinatarios if d]
    excluir_list = excluir if excluir else []
    visited_groups = set()

    async def _get_excluir_ids():
        if not excluir_list: return set()
        try:
            res = await _directus_request("/items/core_notifier_clients", {
                "filter": json.dumps({"code": {"_in": excluir_list}}),
                "fields": "id"
            })
            return {str(r["id"]) for r in res}
        except Exception as e:
            logger.warning(f"Error al obtener IDs para excluir: {e}")
            return set()

    async def _resolve_group(group_id: str, depth: int = 0):
        if group_id in visited_groups or depth > 5: return
        visited_groups.add(group_id)
        
        try:
            members = await _directus_request("/items/core_notification_group_members", {
                "filter": json.dumps({"group_id": {"_eq": group_id}}),
                "fields": "notifier_client_id.core_notifier_clients_id.id,notifier_client_id.core_notifier_clients_id.code,store_id,subgroup_id.core_notification_groups_id"
            })
            
            store_ids = []
            subgroup_ids = []
            
            for m in members:
                # Cliente directo (M2M relation)
                rc_list = m.get("notifier_client_id")
                if rc_list and isinstance(rc_list, list):
                    for rc_item in rc_list:
                        client_info = rc_item.get("core_notifier_clients_id")
                        if client_info:
                            cid = str(client_info.get("id"))
                            code = client_info.get("code")
                            client_ids.add(cid)
                            if code: id_to_code[cid] = str(code)
                
                # Colectar Tiendas y Subgrupos para batch
                rs = m.get("store_id")
                if rs: store_ids.append(str(rs.get("id") if isinstance(rs, dict) else rs))
                
                # Subgrupos (M2M relation)
                rg_list = m.get("subgroup_id")
                if rg_list and isinstance(rg_list, list):
                    for rg_item in rg_list:
                        sg_info = rg_item.get("core_notification_groups_id")
                        if sg_info:
                            sg_id = str(sg_info.get("id") if isinstance(sg_info, dict) else sg_info)
                            subgroup_ids.append(sg_id)
            
            # Resolver Tiendas en batch
            if store_ids:
                clients = await _directus_request("/items/core_notifier_clients", {
                    "filter": json.dumps({"store_id": {"_in": store_ids}}),
                    "fields": "id,code"
                })
                for c in clients:
                    cid = str(c["id"])
                    client_ids.add(cid)
                    if c.get("code"): id_to_code[cid] = str(c["code"])
            
            # Resolver Subgrupos (recursión protegida)
            if subgroup_ids:
                tasks = [_resolve_group(sg_id, depth + 1) for sg_id in subgroup_ids]
                await asyncio.gather(*tasks)
                    
        except Exception as e:
            logger.error(f"Error resolviendo grupo {group_id}: {e}")

    # Separar destinatarios por tipo para procesamiento en lote
    codigos_directos = []
    usuarios_solo = []
    grupos_nombres = []
    areas_nombres = []
    tienda_ids = []

    for d in destinos_raw:
        d_lower = d.lower()
        if d_lower == "todos":
            res = await _directus_request("/items/core_notifier_clients", {"fields": "id,code", "limit": "-1"})
            for c in res:
                cid = str(c["id"])
                client_ids.add(cid)
                if c.get("code"): id_to_code[cid] = str(c["code"])
        elif d_lower.startswith("grupo:"):
            grupos_nombres.append(d[6:].strip())
        elif d_lower.startswith("area:"):
            areas_nombres.append(d[5:].strip())
        elif d_lower.startswith("tienda:"):
            tienda_ids.append(d[7:].strip())
        elif d_lower.startswith("store:"):
            tienda_ids.append(d[6:].strip())
        elif d_lower.startswith("usuario:"):
            usuarios_solo.append(d[8:].strip())
        elif d_lower.startswith("user:"):
            usuarios_solo.append(d[5:].strip())
        else:
            codigos_directos.append(d)

    # 1. Resolver Códigos Directos generales
    if codigos_directos:
        res = await _directus_request("/items/core_notifier_clients", {
            "filter": json.dumps({"code": {"_in": codigos_directos}}),
            "fields": "id,code,name"
        })
        for c in res:
            cid = str(c["id"])
            client_ids.add(cid)
            name_part = f" ({c.get('name', '')})" if c.get('name') else ""
            id_to_code[cid] = f"{c.get('code', '???')}{name_part}"

    # 1b. Resolver Usuarios Normales (que NO son tienda / store_id es nulo)
    if usuarios_solo:
        res = await _directus_request("/items/core_notifier_clients", {
            "filter": json.dumps({
                "code": {"_in": usuarios_solo},
                "store_id": {"_null": True}
            }),
            "fields": "id,code,name"
        })
        for c in res:
            cid = str(c["id"])
            client_ids.add(cid)
            name_part = f" ({c.get('name', '')})" if c.get('name') else ""
            id_to_code[cid] = f"{c.get('code', '???')}{name_part}"

    # 1c. Resolver Tiendas estrictamente por store_id
    if tienda_ids:
        # Generar lista con cadenas y enteros para coincidir exactamente con el campo store_id en Directus
        store_filter = []
        for tid in tienda_ids:
            store_filter.append(tid)
            if tid.isdigit():
                store_filter.append(int(tid))

        res = await _directus_request("/items/core_notifier_clients", {
            "filter": json.dumps({
                "store_id": {"_in": store_filter}
            }),
            "fields": "id,code,name"
        })
        for c in res:
            cid = str(c["id"])
            client_ids.add(cid)
            name_part = f" (Tienda {c.get('name', '')})" if c.get('name') else ""
            id_to_code[cid] = f"{c.get('code', '???')}{name_part}"

    # 2. Resolver Áreas en batch
    if areas_nombres:
        users = await _directus_request("/users", {
            "filter": json.dumps({"area": {"_in": areas_nombres}}),
            "fields": "id"
        })
        u_ids = [str(u["id"]) for u in users]
        if u_ids:
            clients = await _directus_request("/items/core_notifier_clients", {
                "filter": json.dumps({"user_id": {"_in": u_ids}}),
                "fields": "id,code"
            })
            for c in clients:
                cid = str(c["id"])
                client_ids.add(cid)
                if c.get("code"): id_to_code[cid] = str(c["code"])

    # 3. Resolver Grupos
    if grupos_nombres:
        groups = await _directus_request("/items/core_notification_groups", {
            "filter": json.dumps({"name": {"_in": grupos_nombres}}),
            "fields": "id"
        })
        tasks = [_resolve_group(str(g["id"])) for g in groups]
        await asyncio.gather(*tasks)

    exclude_ids = await _get_excluir_ids()
    final_ids = [tid for tid in client_ids if tid not in exclude_ids]
    
    return final_ids, destinos_raw, id_to_code


async def deliver_pending_notifications(client_code: str, client_id: str, websocket) -> int:
    """Entrega notificaciones pendientes (Async & Batch)."""
    now = now_colombia()
    
    try:
        now_naive = now.replace(tzinfo=None).isoformat()
        filter_obj = {
            "client_id": {"_eq": client_id},
            "is_delivered": {"_eq": False},
            "scheduled_date": {"_lte": now_naive},
            "_or": [
                {"expiration_date": {"_gt": now_naive}},
                {"expiration_date": {"_null": True}}
            ]
        }
        
        pending = await _directus_request("/items/core_notifications_pending", {
            "filter": json.dumps(filter_obj),
            "fields": "id,notification_id"
        })
        
        if not pending: return 0

        # Obtener detalles en batch
        notif_ids = list(set(str(p["notification_id"]) for p in pending if p.get("notification_id")))
        notifs = await _directus_request("/items/core_notifications", {
            "filter[id][_in]": ",".join(notif_ids),
            "fields": "*"
        })
        notif_map = {str(n["id"]): n for n in notifs}

        # Enviar resumen
        await websocket.send_json({
            "titulo": "Notificaciones pendientes",
            "mensaje": f"Tienes {len(pending)} notificación(es) nueva(s).",
            "tipo": "info",
            "duracion_seg": 10
        })

        delivered_ids = []
        client = await get_async_client()
        
        for p in pending:
            n_data = notif_map.get(str(p["notification_id"]))
            if not n_data: continue
            
            try:
                await websocket.send_json({
                    "titulo": n_data.get("title", "Notificación"),
                    "mensaje": n_data.get("message", ""),
                    "tipo": n_data.get("notification_type", "info"),
                    "duracion_seg": n_data.get("duration_seconds", 15),
                    "persistente": n_data.get("is_persistent", False),
                    "mostrar_boton_cerrar": n_data.get("show_close_button", True),
                    "pausar_al_hover": n_data.get("pause_on_hover", True),
                    "ruta_accion": n_data.get("action_route")
                })
                delivered_ids.append(p["id"])
            except Exception as e:
                logger.error(f"Error enviando notificación {p['id']} vía WS: {e}")

        # Marcar todas como entregadas en una sola petición (Batch Patch)
        if delivered_ids:
            try:
                await client.patch("/items/core_notifications_pending", json={
                    "keys": delivered_ids,
                    "data": {"is_delivered": True}
                })
                return len(delivered_ids)
            except Exception as e:
                logger.error(f"Error en batch-patch de entregados: {e}")
                return 0
        
        return 0

    except Exception as e:
        logger.error(f"Error en deliver_pending_notifications: {e}")
        return 0

async def save_notification_log(
    titulo: str, mensaje: str, tipo: str, remitente: str, ip_origen: str,
    destinos_raw: list, destinos_reales: list, enviados: int, pendientes: int,
    duracion_seg: int = 15, persistente: bool = False,
    mostrar_boton_cerrar: bool = True, pausar_al_hover: bool = True,
    scheduled_date: Optional[str] = None, ruta_accion: Optional[str] = None
) -> Optional[str]:
    """Guarda el log de notificación en Directus (Async)."""
    try:
        res = await (await get_async_client()).post("/items/core_notifications", json={
            "title": titulo,
            "message": mensaje,
            "notification_type": tipo,
            "sender_name": remitente,
            "source_ip": ip_origen,
            "destinations_raw": destinos_raw,
            "actual_destinations": destinos_reales,
            "sent_count": enviados,
            "pending_count": pendientes,
            "is_persistent": persistente,
            "duration_seconds": duracion_seg,
            "show_close_button": mostrar_boton_cerrar,
            "pause_on_hover": pausar_al_hover,
            "scheduled_date": scheduled_date,
            "action_route": ruta_accion,
            "sent_at": now_colombia().isoformat()
        })
        return res.json().get("data", {}).get("id")
    except Exception as e:
        logger.error(f"Error save_notification_log: {e}")
        return None

async def save_pending_notifications(client_ids: List[str], notification_id: str, scheduled_date: Optional[str] = None):
    """Guarda múltiples notificaciones pendientes en batch (Async)."""
    if not client_ids: return
    
    now = now_colombia()
    now_naive_str = now.replace(tzinfo=None).isoformat()
    final_scheduled_date = scheduled_date if scheduled_date else now_naive_str
    
    # Obtener TTL de config (cacheable o una sola vez)
    ttl_hours = 24
    try:
        config = await _directus_request("/items/core_notification_configs", {
            "filter": json.dumps({"is_active": {"_eq": True}}),
            "fields": "pending_tll_hours",
            "limit": 1
        })
        if config: ttl_hours = config[0].get("pending_tll_hours", 24)
    except: pass

    # Solo aplicamos expiración si es una notificación instantánea (sin scheduled_date previo)
    expiration_date = None
    if not scheduled_date:
        expiration_date = (now.replace(tzinfo=None) + timedelta(hours=ttl_hours)).isoformat()
    
    client = await get_async_client()
    
    # Directus soporta creación masiva pasando una lista al endpoint /items/collection
    items = [{
        "client_id": cid,
        "notification_id": notification_id,
        "expiration_date": expiration_date,
        "scheduled_date": final_scheduled_date,
        "is_delivered": False
    } for cid in client_ids]
    
    try:
        response = await client.post("/items/core_notifications_pending", json=items)
        created = response.json().get("data", [])
        logger.info(f"[✓] {len(created)} pendientes guardadas en batch")
        return created
    except Exception as e:
        logger.error(f"Error guardando pendientes batch: {e}")
        return []

