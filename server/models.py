import os
from datetime import datetime, timedelta
from typing import List, Optional
from pydantic import BaseModel, ValidationError, validator
from datetime import datetime
from timestamp_utils import validate_timestamp  # Se preferisci spostare la validazione timestamp in un file separato

# Mappatura campi -> id anomalia
FIELD_TO_ANOMALIA_ID = {
    'timestamp_fine': 1,
    'peso_effettivo': 2,
    'temperatura_effettiva': 3,
    'anomalia': 4
}

class Anomalia(BaseModel):
    id: int

class Operazione(BaseModel):
    """
    Modello Pydantic per validare i dati di una operazione.
    """
    id_ordine: int
    codice_pezzo: str
    codice_macchinario: str
    codice_operatore: str
    timestamp_inizio: datetime
    timestamp_fine: datetime
    tipo_operazione: str
    peso_effettivo: Optional[float] = None
    temperatura_effettiva: Optional[float] = None
    numero_pezzi_ora: Optional[int] = None
    tipo_fermo: Optional[str] = None
    anomalia: Optional[List[Anomalia]] = None

    @validator('tipo_operazione')
    def tipo_operazione_valido(cls, v):
        if v not in {'forgiatura', 'cnc'}:
            raise ValueError('Tipo operazione non riconosciuto')
        return v

    @validator('peso_effettivo')
    def peso_effettivo_range(cls, v, values):
        """
        Controlla che il peso effettivo sia presente e in un certo range
        solo se la tipo_operazione è 'forgiatura'.
        """
        if values.get('tipo_operazione') == 'forgiatura':
            if v is None or not (0 <= v <= 1000):
                raise ValueError('Peso fuori range')
        return v

    @validator('temperatura_effettiva')
    def temperatura_effettiva_range(cls, v, values):
        """
        Controlla che la temperatura effettiva sia presente e in un certo range
        solo se la tipo_operazione è 'forgiatura'.
        """
        if values.get('tipo_operazione') == 'forgiatura':
            if v is None or not (-50 <= v <= 150):
                raise ValueError('Temperatura fuori range')
        return v

    @validator('timestamp_inizio', 'timestamp_fine', pre=True)
    def validate_and_parse_timestamp(cls, v, field):
        """
        Valida e parsifica i timestamp usando la funzione `validate_timestamp`.
        """
        try:
            return validate_timestamp(v)
        except ValueError as e:
            raise ValueError(f"{field.name} non valido: {e}")
    
    @validator('anomalia', each_item=True)
    def anomalia_valida(cls, v):
        if not isinstance(v, Anomalia):
            raise ValueError('Struttura anomalia non valida')
        return v