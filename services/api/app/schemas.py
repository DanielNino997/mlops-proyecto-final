"""Esquemas de entrada/salida de la API (validación con Pydantic)."""
from datetime import date
from typing import Optional

from pydantic import BaseModel, Field


class PropertyIn(BaseModel):
    bed: float = Field(ge=0, le=100, description="Número de habitaciones")
    bath: float = Field(ge=0, le=100, description="Número de baños")
    acre_lot: float = Field(ge=0, description="Tamaño del terreno en acres")
    house_size: float = Field(gt=0, description="Área habitable en pies cuadrados")
    city: str = Field(min_length=1)
    state: str = Field(min_length=1)
    zip_code: str = Field(pattern=r"^\d{3,5}$", description="Código postal (se completa a 5 dígitos)")
    status: str = Field(default="for_sale", description="for_sale, ready_to_build o sold")
    brokered_by: Optional[str] = Field(default=None, description="Código de la agencia, si se conoce")
    prev_sold_date: Optional[date] = Field(default=None, description="Fecha de venta anterior, si existe")

    model_config = {"json_schema_extra": {"example": {
        "bed": 3, "bath": 2, "acre_lot": 0.25, "house_size": 1600, "city": "Austin", "state": "Texas",
        "zip_code": "78701", "status": "for_sale", "brokered_by": "101640", "prev_sold_date": "2015-06-30"}}}


class PredictionOut(BaseModel):
    predicted_price: float
    model_name: str
    model_version: str
    latency_ms: float
