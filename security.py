import logging
import time
from typing import Dict, List

logger = logging.getLogger("security")

class SecurityManager:
    """Gestiona seguridad del servidor: rate limiting y bloqueo de IPs."""

    def __init__(self,
                 max_attempts: int = 10,
                 block_duration: int = 300,
                 max_conn_per_min: int = 30):
        self.max_attempts = max_attempts
        self.block_duration = block_duration
        self.max_conn_per_min = max_conn_per_min

        self._failed_attempts: Dict[str, List[float]] = {}
        self._connection_log: Dict[str, List[float]] = {}
        self._blocked_ips: Dict[str, float] = {}

    def is_ip_blocked(self, ip: str) -> bool:
        """Verifica si una IP está bloqueada."""
        now = time.time()
        if ip in self._blocked_ips:
            if now < self._blocked_ips[ip]:
                return True
            del self._blocked_ips[ip]
            if ip in self._failed_attempts: del self._failed_attempts[ip]
            logger.info(f"IP desbloqueada: {ip}")
        return False

    def record_failed_attempt(self, ip: str):
        """Registra un intento fallido."""
        now = time.time()
        if ip not in self._failed_attempts: self._failed_attempts[ip] = []
        self._failed_attempts[ip].append(now)

        # Limpiar viejos
        cutoff = now - self.block_duration
        self._failed_attempts[ip] = [t for t in self._failed_attempts[ip] if t > cutoff]

        if len(self._failed_attempts[ip]) >= self.max_attempts:
            self._blocked_ips[ip] = now + self.block_duration
            logger.warning(f"IP bloqueada por {self.block_duration}s: {ip} ({len(self._failed_attempts[ip])} fallos)")

    def check_rate_limit(self, ip: str) -> bool:
        """Verifica conexiones por minuto."""
        now = time.time()
        cutoff = now - 60
        
        if ip not in self._connection_log: self._connection_log[ip] = []
        self._connection_log[ip] = [t for t in self._connection_log[ip] if t > cutoff]

        if len(self._connection_log[ip]) >= self.max_conn_per_min:
            logger.warning(f"Rate limit excedido para {ip}")
            return False

        self._connection_log[ip].append(now)
        return True

    def record_successful_connection(self, ip: str):
        """Limpia historial de fallos."""
        if ip in self._failed_attempts: del self._failed_attempts[ip]

    def get_stats(self) -> dict:
        now = time.time()
        return {
            "active_blocks": sum(1 for exp in self._blocked_ips.values() if exp > now),
            "ips_with_failed_attempts": len(self._failed_attempts)
        }

# Instancia global única
security = SecurityManager()
