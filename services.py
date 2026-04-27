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
        e_ids = set()
        for ex in excluir_list:
            try:
                res = await _directus_request("/items/core_notifier_clients", {
                    "filter[code][_eq]": ex, 
                    "fields": "id"
                })
                for r in res: e_ids.add(str(r["id"]))
            except: pass
        return e_ids

    async def _resolve_group(group_id: str, depth: int = 0):
        if group_id in visited_groups or depth > 5: return
        visited_groups.add(group_id)
        
        try:
            members = await _directus_request("/items/core_notification_group_members", {
                "filter[group_id][_eq]": group_id,
                "fields": "notifier_client_id.*,store_id.*,subgroup_id.*"
            })
            
            for m in members:
                # Cliente directo
                rc = m.get("notifier_client_id")
                if rc:
                    its = rc if isinstance(rc, list) else [rc]
                    for it in its:
                        if isinstance(it, dict):
                            cid = str(it.get("id"))
                            code = it.get("code")
                            if cid:
                                client_ids.add(cid)
                                if code: id_to_code[cid] = str(code)
                
                # Por Tienda
                rs = m.get("store_id")
                if rs:
                    s_id = str(rs.get("id") if isinstance(rs, dict) else rs)
                    clients = await _directus_request("/items/core_notifier_clients", {
                        "filter[store_id][_eq]": s_id, 
                        "fields": "id,code"
                    })
                    for c in clients:
                        cid = str(c["id"])
                        client_ids.add(cid)
                        if c.get("code"): id_to_code[cid] = str(c["code"])
                
                # Subgrupos (recursión protegida)
                rg = m.get("subgroup_id")
                if rg:
                    sg_id = str(rg.get("id") if isinstance(rg, dict) else rg)
                    await _resolve_group(sg_id, depth + 1)
                    
        except Exception as e:
            logger.error(f"Error resolviendo grupo {group_id}: {e}")

    # Procesar destinatarios
    for d in destinos_raw:
        if d.lower() == "todos":
            res = await _directus_request("/items/core_notifier_clients", {"fields": "id,code", "limit": "-1"})
            for c in res:
                cid = str(c["id"])
                client_ids.add(cid)
                if c.get("code"): id_to_code[cid] = str(c["code"])
        
        elif d.startswith("grupo:"):
            gname = d.replace("grupo:", "").strip()
            groups = await _directus_request("/items/core_notification_groups", {"filter[name][_eq]": gname, "fields": "id"})
            for g in groups: await _resolve_group(str(g["id"]))
            
        elif d.startswith("area:"):
            area = d.replace("area:", "").strip()
            users = await _directus_request("/users", {"filter[area][_eq]": area, "fields": "id"})
            u_ids = [str(u["id"]) for u in users]
            if u_ids:
                clients = await _directus_request("/items/core_notifier_clients", {
                    "filter[user_id][_in]": ",".join(u_ids), 
                    "fields": "id,code"
                })
                for c in clients:
                    cid = str(c["id"])
                    client_ids.add(cid)
                    if c.get("code"): id_to_code[cid] = str(c["code"])
        
        else: # Código directo
            res = await _directus_request("/items/core_notifier_clients", {"filter[code][_eq]": d, "fields": "id,code,name"})
            if res:
                for c in res:
                    cid = str(c["id"])
                    client_ids.add(cid)
                    id_to_code[cid] = f"{c.get('code', d)} ({c.get('name', '')})".strip(" ()")
            else:
                client_ids.add(d)

    exclude_ids = await _get_excluir_ids()
    final_ids = [tid for tid in client_ids if tid not in exclude_ids]
    
    return final_ids, destinos_raw, id_to_code

async def deliver_pending_notifications(client_code: str, client_id: str, websocket) -> int:
    """Entrega notificaciones pendientes (Async & Batch)."""
    now = now_colombia()
    
    try:
        filter_obj = {
            "client_id": {"_eq": client_id},
            "is_delivered": {"_eq": False},
            "_or": [
                {"expiration_date": {"_gt": now.isoformat()}},
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

        delivered = 0
        client = get_async_client()
        
        for p in pending:
            n_data = notif_map.get(str(p["notification_id"]))
            if not n_data: continue
            
            try:
                await websocket.send_json({
                    "titulo": n_data.get("title", "Notificación"),
                    "mensaje": n_data.get("message", ""),
                    "tipo": n_data.get("notification_type", "info"),
                    "duracion_seg": n_data.get("duration_seconds", 15),
                    "persistente": n_data.get("is_persistent", False)
                })
                
                await client.patch(f"/items/core_notifications_pending/{p['id']}", json={"is_delivered": True})
                delivered += 1
            except Exception as e:
                logger.error(f"Error entregando pendiente {p['id']}: {e}")

        return delivered
    except Exception as e:
        logger.error(f"Error en deliver_pending_notifications: {e}")
        return 0

async def save_notification_log(
    titulo: str, mensaje: str, tipo: str, remitente: str, ip_origen: str,
    destinos_raw: list, destinos_reales: list, enviados: int, pendientes: int,
    duracion_seg: int = 15, persistente: bool = False
) -> Optional[str]:
    """Guarda el log de notificación en Directus (Async)."""
    try:
        res = await get_async_client().post("/items/core_notifications", json={
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
            "sent_at": now_colombia().isoformat()
        })
        return res.json().get("data", {}).get("id")
    except Exception as e:
        logger.error(f"Error save_notification_log: {e}")
        return None

async def save_pending_notifications(client_ids: List[str], notification_id: str):
    """Guarda múltiples notificaciones pendientes en batch (Async)."""
    if not client_ids: return
    
    # Obtener TTL de config (cacheable o una sola vez)
    ttl_hours = 24
    try:
        config = await _directus_request("/items/core_notification_configs", {
            "filter[is_active][_eq]": True, "fields": "pending_ttl_hours", "limit": 1
        })
        if config: ttl_hours = config[0].get("pending_ttl_hours", 24)
    except: pass

    exp = (now_colombia() + timedelta(hours=ttl_hours)).isoformat()
    client = get_async_client()
    
    # Directus soporta creación masiva pasando una lista al endpoint /items/collection
    items = [{
        "client_id": cid,
        "notification_id": notification_id,
        "expiration_date": exp,
        "is_delivered": False
    } for cid in client_ids]
    
    try:
        await client.post("/items/core_notifications_pending", json=items)
        logger.info(f"[✓] {len(items)} pendientes guardadas en batch")
    except Exception as e:
        logger.error(f"Error guardando pendientes batch: {e}")
