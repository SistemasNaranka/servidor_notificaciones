"""
Módulo de autenticación y gestión de clientes con Directus (Async).
"""

import logging
import httpx
import hashlib
from typing import Optional, Any
from config import DIRECTUS_URL, DIRECTUS_TOKEN, DIRECTUS_VERIFY_SSL
from utils import now_colombia

logger = logging.getLogger(__name__)

# Cliente async persistente para reutilizar conexiones
_async_client: Optional[httpx.AsyncClient] = None

def get_async_client() -> httpx.AsyncClient:
    """Obtiene o crea el cliente HTTP asíncrono."""
    global _async_client
    if _async_client is None:
        _async_client = httpx.AsyncClient(
            base_url=DIRECTUS_URL,
            headers={"Authorization": f"Bearer {DIRECTUS_TOKEN}"},
            verify=DIRECTUS_VERIFY_SSL,
            timeout=10.0
        )
    return _async_client

async def _directus_request(endpoint: str, params: dict = None) -> list:
    """Realiza una petición GET asíncrona a Directus."""
    client = get_async_client()
    try:
        response = await client.get(endpoint, params=params)
        response.raise_for_status()
        data = response.json()
        return data.get("data", [])
    except Exception as e:
        logger.error(f"Error en _directus_request ({endpoint}): {e}")
        return []

async def validate_token(token: str) -> Optional[dict]:
    """Capa 1: Valida si el token es válido en Directus."""
    try:
        async with httpx.AsyncClient(verify=DIRECTUS_VERIFY_SSL, timeout=5.0) as client:
            response = await client.get(
                f"{DIRECTUS_URL.rstrip('/')}/users/me",
                headers={"Authorization": f"Bearer {token}"},
                params={"fields": "id,email,first_name,last_name"}
            )
            if response.status_code == 200:
                return response.json().get("data")
            return None
    except Exception as e:
        logger.warning(f"Token inválido en Directus: {e}")
        return None

async def get_client_by_token(token: str) -> Optional[dict]:
    """Busca cliente por token en core_notifier_clients."""
    results = await _directus_request("/items/core_notifier_clients", params={
        "filter[token][_eq]": token,
        "fields": "*",
        "limit": 1
    })
    return results[0] if results else None

async def find_user_by_token(token: str) -> Optional[dict]:
    """Capa 2b: Busca el usuario en directus_users por token."""
    return await validate_token(token)

async def auto_register_client(user_data: dict, token: str = None, version: str = "1.0.0") -> Optional[dict]:
    """Auto-registro en core_notifier_clients (Async)."""
    try:
        client = get_async_client()
        user_id = user_data.get("id")
        email = user_data.get("email")
        
        if not user_id:
            return None

        # Recuperar datos completos si faltan
        if not email:
            try:
                res = await client.get(f"/users/{user_id}")
                full_user = res.json().get("data", {})
                email = full_user.get("email")
                user_data.update(full_user)
            except Exception as e:
                logger.warning(f"No se pudo recuperar email del usuario {user_id}: {e}")

        if not email:
            logger.warning(f"Registro cancelado: No se pudo determinar email para user_id={user_id}")
            return None

        name = f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}".strip() or email
        code = int(hashlib.sha256(str(user_id).encode()).hexdigest(), 16) % 10000000 # Espacio mayor

        # Verificar existencia previa
        existing = await _directus_request("/items/core_notifier_clients", params={
            "filter[user_id][_eq]": str(user_id),
            "fields": "id,user_id,code,is_active,token,version",
            "limit": 1
        })
        
        if existing:
            existing_client = existing[0]
            updates = {}
            if existing_client.get("token") != token: updates["token"] = token
            if existing_client.get("version") != version: updates["version"] = version
            if not existing_client.get("is_active"): updates["is_active"] = True
            
            if updates:
                await client.patch(f"/items/core_notifier_clients/{existing_client['id']}", json=updates)
                existing_client.update(updates)
            
            return existing_client

        # Crear nuevo
        nuevo = {
            "code": code,
            "name": name,
            "user_id": user_id,
            "token": token,
            "version": version,
            "is_active": True,
            "last_ping": now_colombia().isoformat()
        }
        
        response = await client.post("/items/core_notifier_clients", json=nuevo)
        return response.json().get("data")

    except Exception as e:
        logger.error(f"Error en auto_register_client: {e}")
        return None

async def authenticate_websocket_token(token: str, version: str = "1.0.0") -> Optional[dict]:
    """Flujo completo de autenticación para WebSocket (Async)."""
    client_record = await get_client_by_token(token)
    
    if not client_record:
        user_data = await find_user_by_token(token)
        if user_data:
            client_record = await auto_register_client(user_data, token, version=version)

    if client_record:
        # Sincronizar estado
        updates = {}
        if client_record.get("version") != version: updates["version"] = version
        if not client_record.get("is_active"): updates["is_active"] = True
        
        if updates:
            try:
                await get_async_client().patch(f"/items/core_notifier_clients/{client_record['id']}", json=updates)
                client_record.update(updates)
            except Exception as e:
                logger.error(f"Error sincronizando cliente: {e}")
        
        return client_record
    
    return None

async def get_client_by_code(code: str) -> Optional[dict]:
    """Busca cliente por código."""
    results = await _directus_request("/items/core_notifier_clients", params={
        "filter[code][_eq]": str(code),
        "fields": "*",
        "limit": 1
    })
    return results[0] if results else None

async def update_client_last_ping(client_id: str) -> bool:
    """Actualiza last_ping del cliente (Async)."""
    try:
        await get_async_client().patch(f"/items/core_notifier_clients/{client_id}", json={
            "last_ping": now_colombia().isoformat()
        })
        return True
    except Exception as e:
        logger.warning(f"Error actualizando last_ping para {client_id}: {e}")
        return False

async def check_directus_connection() -> bool:
    """Verifica conexión con Directus."""
    try:
        res = await get_async_client().get("/items/core_notifier_clients", params={"limit": 1})
        return res.status_code == 200
    except Exception:
        return False
