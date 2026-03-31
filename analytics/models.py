from dataclasses import dataclass
from typing import Optional


@dataclass
class Categoria:
    id: int
    nombre: str
    color: str
    activa: bool = True


@dataclass
class Presupuesto:
    categoria_id: int
    periodo: str  # "YYYY-MM"
    monto: float


@dataclass
class Clasificacion:
    codigo_autorizacion: Optional[str]
    tx_hash: Optional[str]
    categoria_id: int
    origen: str = "manual"  # 'manual' | 'auto'
