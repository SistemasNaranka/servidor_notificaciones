from pydantic import BaseModel, Field, validator
from typing import List, Optional
from datetime import datetime

class NotificationRequest(BaseModel):
    destinatarios: List[str] = Field(..., min_items=1)
    titulo: str = Field("Notificación", max_length=100)
    mensaje: str = Field("", max_length=1000)
    tipo: str = Field("info")
    duracion_seg: int = Field(15, ge=1, le=3600)
    persistente: bool = False
    clickeable: bool = True
    mostrar_boton_cerrar: bool = True
    pausar_al_hover: bool = True
    ruta_accion: Optional[str] = None
    excluir: List[str] = []
    fecha_programada: Optional[str] = None

    @validator("tipo")
    def validate_tipo(cls, v):
        allowed = ["info", "success", "warning", "error", "info_dark"]
        if v not in allowed:
            raise ValueError(f"Tipo debe ser uno de: {allowed}")
        return v
