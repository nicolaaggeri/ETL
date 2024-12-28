import os
import math
import json
import shutil
import logging
import mysql.connector
import psycopg2
import psycopg2.extras

from datetime import datetime, timedelta, date
from threading import Thread
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask_cors import CORS
from functools import wraps
from mysql.connector import Error
from pydantic import BaseModel, ValidationError, validator
from typing import List, Optional

###############################################################################
#                           CONFIGURAZIONE LOGGING                            #
###############################################################################

def configure_logging():
    """
    Configura i logger per l'applicazione:
    - logger principale (etl.log)
    - logger periodico (periodic.log)
    - logger PostgreSQL (postgresql.log)
    - logger MySQL (mysql.log)
    """
    # Logger principale
    logging.basicConfig(
        filename='logs/etl.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Logger periodico
    periodic_logger = logging.getLogger('periodic_logger')
    periodic_logger.setLevel(logging.INFO)
    periodic_handler = logging.FileHandler('logs/periodic.log')
    periodic_handler.setLevel(logging.INFO)
    periodic_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    periodic_handler.setFormatter(periodic_formatter)
    periodic_logger.addHandler(periodic_handler)

    # Logger PostgreSQL
    postgres_logger = logging.getLogger('postgresql_logger')
    postgres_logger.setLevel(logging.INFO)
    postgres_handler = logging.FileHandler('logs/postgresql.log')
    postgres_handler.setLevel(logging.INFO)
    postgres_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    postgres_handler.setFormatter(postgres_formatter)
    postgres_logger.addHandler(postgres_handler)

    # Logger MySQL
    mysql_logger = logging.getLogger('mysql_logger')
    mysql_logger.setLevel(logging.INFO)
    mysql_handler = logging.FileHandler('logs/mysql.log')
    mysql_handler.setLevel(logging.INFO)
    mysql_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    mysql_handler.setFormatter(mysql_formatter)
    mysql_logger.addHandler(mysql_handler)

    return periodic_logger, postgres_logger, mysql_logger

periodic_logger, postgres_logger, mysql_logger = configure_logging()

###############################################################################
#                          CONFIGURAZIONE PYDANTIC                            #
###############################################################################

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
    id_ordine: Optional[int]
    codice_pezzo: Optional[str]
    codice_macchinario: Optional[str]
    codice_operatore: Optional[str]
    timestamp_inizio: Optional[datetime]
    timestamp_fine: Optional[datetime]
    tipo_operazione: Optional[str]
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
            if v is None or not (v < 700 or v > 1200):
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

###############################################################################
#                       CONFIGURAZIONE AMBIENTE E FLASK                       #
###############################################################################
load_dotenv(dotenv_path='config/.env')  # Caricamento variabili ambiente

app = Flask(__name__)
CORS(app)

etl_status = {
    'running': False,
    'last_run': None,
    'last_success': None,
    'last_error': None
}

MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = os.getenv('MYSQL_PORT')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')

POSTGRES_HOST = os.getenv('PG_HOST')
POSTGRES_PORT = os.getenv('PG_PORT')
POSTGRES_DATABASE = os.getenv('PG_DATABASE')
POSTGRES_USER = os.getenv('PG_USER')
POSTGRES_PASSWORD = os.getenv('PG_PASSWORD')

###############################################################################
#                         FUNZIONI DI UTILITÀ (LOG E TIMESTAMP)               #
###############################################################################
def clear_log_file(log_file_path: str, backup: bool = True) -> None:
    """
    Pulisce il file di log, mantenendo opzionalmente una copia di backup.
    """
    try:
        if backup and os.path.exists(log_file_path):
            backup_path = f"{log_file_path}.backup"
            shutil.copy(log_file_path, backup_path)
            logging.info(f"Backup del file di log creato: {backup_path}")

        with open(log_file_path, 'w') as log_file:
            log_file.truncate(0)
            logging.info(f"File di log '{log_file_path}' svuotato con successo.")
    except FileNotFoundError:
        logging.warning(f"Il file di log '{log_file_path}' non è stato trovato.")
    except Exception as e:
        logging.error(f"Errore durante la pulizia del file di log '{log_file_path}': {e}")

def toggle_foreign_keys(cursor, enable: bool) -> None:
    """
    Abilita o disabilita i foreign key checks.
    """
    if enable:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        logging.info("Foreign key checks riabilitati.")
    else:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        logging.info("Foreign key checks disabilitati.")

def parse_timestamp(timestamp_str: str) -> datetime:
    """
    Tenta di parsare una stringa timestamp con o senza microsecondi.
    """
    try:
        return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')

def validate_timestamp(timestamp, tolerance_minutes: int = 60) -> datetime:
    """
    Valida che il timestamp sia entro una certa tolleranza rispetto all'orario corrente.
    Supporta sia stringhe che oggetti datetime.
    """
    if isinstance(timestamp, datetime):
        parsed_timestamp = timestamp
    else:
        parsed_timestamp = parse_timestamp(timestamp)

    now = datetime.utcnow()
    tolerance = timedelta(minutes=tolerance_minutes)

    if parsed_timestamp > now + tolerance:
        raise ValueError(f"Timestamp fuori dalla tolleranza di {tolerance_minutes} minuti")

    return parsed_timestamp

###############################################################################
#                      CONNESSIONE E FUNZIONI AL DATABASE                     #
###############################################################################
def connect_to_db():
    """
    Crea e ritorna una connessione al database MySQL.
    """
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            database=MYSQL_DATABASE,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD
        )
        return conn
    except Error as e:
        logging.error(f"Errore nella connessione al database MySQL: {e}")
        raise

def connect_to_db_postgres():
    """
    Crea e ritorna una connessione al database PostgreSQL.
    """
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DATABASE,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        return conn
    except psycopg2.Error as e:
        logging.error(f"Errore nella connessione al database PostgreSQL: {e}")
        raise

def insert_operation_data(connection, cursor, data: dict) -> (bool, str, int):
    """
    Inserisce l'operazione e gli eventuali dettagli (forgiatura o cnc) e anomalie.
    Ritorna (success, error_message, id_operazione).
    """
    try:
        # Inserimento operazione
        insert_operazione = """
            INSERT INTO operazioni (
                id_ordine, codice_pezzo, codice_macchinario, codice_operatore,
                timestamp_inizio, timestamp_fine
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_operazione, (
            data['id_ordine'],
            data['codice_pezzo'],
            data['codice_macchinario'],
            data['codice_operatore'],
            data['timestamp_inizio'],
            data['timestamp_fine']
        ))
        id_operazione = cursor.lastrowid

        # Inserimento dettagli a seconda del tipo di operazione
        if data['tipo_operazione'] == 'forgiatura':
            insert_forgiatura = """
                INSERT INTO forgiatura (id_operazione, peso_effettivo, temperatura_effettiva, id_anomalia)
                VALUES (%s, %s, %s, NULL)
            """
            cursor.execute(insert_forgiatura, (
                id_operazione,
                data.get('peso_effettivo'),
                data.get('temperatura_effettiva')
            ))

        elif data['tipo_operazione'] == 'cnc':
            insert_cnc = """
                INSERT INTO cnc (id_operazione, numero_pezzi_ora, tipo_fermo)
                VALUES (%s, %s, %s)
            """
            cursor.execute(insert_cnc, (
                id_operazione,
                data.get('numero_pezzi_ora'),
                data.get('tipo_fermo')
            ))
        else:
            return False, "Tipo operazione non riconosciuto", None

        # Inserimento anomalie se presenti
        if 'anomalia' in data and isinstance(data['anomalia'], list) and data['anomalia']:
            insert_anomalia_operazione = """
                INSERT INTO anomalia_operazione (id_anomalia, id_operazione, note)
                VALUES (%s, %s, %s)
            """
            for anomaly in data['anomalia']:
                anomaly_id = anomaly['id']
                cursor.execute(insert_anomalia_operazione, (anomaly_id, id_operazione, 'Anomalia registrata'))

        # Commit delle modifiche
        connection.commit()
        return True, None, id_operazione

    except Exception as e:
        logging.error(f"Errore durante l'inserimento nel database MySQL: {e}", exc_info=True)
        connection.rollback()  # Rollback delle modifiche in caso di errore
        return False, str(e), None

def decrement_quantita_pezzo_ordine(cursor, id_ordine: int, codice_pezzo: str) -> None:
    """
    Decrementa di 1 la quantita_rimanente per un dato ordine e pezzo.
    """
    update_quantita_query = """
        UPDATE pezzi_ordine
        SET quantita_rimanente = quantita_rimanente - 1
        WHERE id_ordine = %s AND id_pezzo = %s
          AND quantita_rimanente < quantita_totale;
    """
    cursor.execute(update_quantita_query, (id_ordine, codice_pezzo))
    logging.info(f"Quantità decrementata per id_ordine={id_ordine}, codice_pezzo={codice_pezzo}")

def save_records(cursor, connection, query, records, retries=3):
    """
    Funzione generica per salvare record nel database con meccanismo di retry.
    """
    logging.info(f"Query: {query}")
    logging.info(f"Esempio di record: {records[:1]}")

    for attempt in range(1, retries + 1):
        try:
            cursor.executemany(query, records)
            connection.commit()
            logging.info(f'Inseriti {cursor.rowcount} record in MySQL al tentativo {attempt}.')
            return True
        except mysql.connector.Error as e:
            logging.error(f'Errore durante l\'inserimento in MySQL al tentativo {attempt}: {e}')
            if connection.is_connected():
                connection.rollback()
            if attempt == retries:
                logging.error('Tutti i tentativi di inserimento in MySQL sono falliti.')
                raise
    return False

###############################################################################
#                          FUNZIONE PRINCIPALE DI ETL POSTGRES                #
###############################################################################
def main_etl_postgres(rows):
    global etl_status
    etl_status['running'] = True
    etl_status['last_run'] = datetime.utcnow().isoformat()

    pg_conn = None
    pg_cursor = None

    try:
        pg_conn = connect_to_db_postgres()
        pg_cursor = pg_conn.cursor()
        postgres_logger.info('Connesso a PostgreSQL.')

        insert_query = """
            INSERT INTO raw_operazione (
                id_ordine, codice_pezzo, codice_macchinario, codice_operatore,
                timestamp_inizio, timestamp_fine, peso_effettivo, temperatura_effettiva,
                id_anomalia, numero_pezzi_ora, tipo_fermo, tipo_operazione
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for i, data in enumerate(rows):
            postgres_logger.debug(f"Ingestione record PostgreSQL #{i}: {data}")
            try:
                insert_values = (
                    data.get('id_ordine'),
                    data.get('codice_pezzo'),
                    data.get('codice_macchinario'),
                    data.get('codice_operatore'),
                    data.get('timestamp_inizio'),
                    data.get('timestamp_fine'),
                    data.get('peso_effettivo'),
                    data.get('temperatura_effettiva'),
                    data['anomalia'][0]['id'] if data.get('anomalia') else None,
                    data.get('numero_pezzi_ora'),
                    data.get('tipo_fermo'),
                    data.get('tipo_operazione')
                )

                pg_cursor.execute(insert_query, insert_values)
                pg_conn.commit()
                postgres_logger.info(f"Record PostgreSQL inserito con successo: ID {pg_cursor.lastrowid}")

            except Exception as e:
                postgres_logger.error(f"Errore durante l'inserimento in PostgreSQL per il record {data}: {e}", exc_info=True)
                pg_conn.rollback()
                etl_status['last_error'] = str(e)
                return 500

        postgres_logger.info("Ingestione ETL PostgreSQL completata con successo.")
        etl_status['last_success'] = datetime.utcnow().isoformat()
        etl_status['last_error'] = None
        return 200

    except Exception as e:
        postgres_logger.error(f'Errore generico ETL PostgreSQL: {e}', exc_info=True)
        periodic_logger.error(f"ETL PostgreSQL fallito con errore: {e}")
        etl_status['last_error'] = str(e)
        return 500

    finally:
        etl_status['running'] = False
        periodic_logger.info("ETL PostgreSQL completato")
        if pg_cursor:
            pg_cursor.close()
        if pg_conn and not pg_conn.closed:
            pg_conn.close()
            postgres_logger.info('Connessione a PostgreSQL chiusa.')

###############################################################################
#                  FUNZIONE VALIDAZIONE E TRASFERIMENTO A MYSQL               #
###############################################################################          
def process_and_transfer_to_mysql():
    global etl_status
    etl_status['running'] = True
    etl_status['last_run'] = datetime.utcnow().isoformat()

    pg_conn = None
    pg_cursor = None
    my_conn = None
    my_cursor = None

    try:
        pg_conn = connect_to_db_postgres()
        pg_cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        postgres_logger.info('Connesso a PostgreSQL per la validazione.')

        my_conn = connect_to_db()
        my_cursor = my_conn.cursor(dictionary=True)
        mysql_logger.info('Connesso a MySQL per il trasferimento.')

        # Seleziona i record che non sono ancora stati processati
        select_query = """
            SELECT *
            FROM raw_operazione
            WHERE stato = 'PENDING';
        """
        pg_cursor.execute(select_query)
        raw_records = pg_cursor.fetchall()
        postgres_logger.info(f"Record RAW trovati: {len(raw_records)}")

        for record in raw_records:
            data = dict(record)
            anomalie = []  # Per raccogliere eventuali anomalie del record

            try:
                # Validazione con Pydantic
                postgres_logger.debug(f"Record da validare: {data}")
                try:
                    operazione = Operazione(**data)
                except ValidationError as ve:
                    errors = ve.errors()
                    postgres_logger.warning(f"Record non valido: {data} - Errori: {errors}")
                    for error in errors:
                        field = error['loc'][-1]
                        message = error['msg']
                        anomaly_id = FIELD_TO_ANOMALIA_ID.get(field, 999)
                        anomalie.append({'id': anomaly_id, 'message': f"{field}: {message}"})
                    # Continua con i campi invalidi forzati a None
                    operazione_dict = {**data, **{err['loc'][-1]: None for err in errors}}
                else:
                    operazione_dict = operazione.dict()

                # Inserimento in MySQL
                success, error_msg, id_operazione = insert_operation_data(my_conn, my_cursor, operazione_dict)
                mysql_logger.debug(f"Tentativo di inserimento in MySQL: {operazione_dict}")
                if success:
                    mysql_logger.info(f"Record inserito con successo in MySQL: ID {id_operazione}")

                    # Registra le anomalie associate
                    if anomalie:
                        insert_anomalia_operazione = """
                            INSERT INTO anomalia_operazione (id_anomalia, id_operazione, note)
                            VALUES (%s, %s, %s)
                        """
                        for anomaly in anomalie:
                            anomaly_id = anomaly['id']
                            message = anomaly['message']
                            my_cursor.execute(insert_anomalia_operazione, (anomaly_id, id_operazione, message))
                        mysql_logger.info(f"Anomalie registrate per ID {id_operazione}: {anomalie}")

                    # Aggiorna lo stato in PostgreSQL
                    update_status_query = """
                        UPDATE raw_operazione
                        SET stato = 'PROCESSED'
                        WHERE id_operazione = %s;
                    """
                    pg_cursor.execute(update_status_query, (record['id_operazione'],))
                    pg_conn.commit()

                    # Cancellazione del record processato
                    delete_query = """
                        DELETE FROM raw_operazione
                        WHERE id_operazione = %s;
                    """
                    pg_cursor.execute(delete_query, (record['id_operazione'],))
                    pg_conn.commit()
                    postgres_logger.info(f"Record con ID {record['id_operazione']} cancellato da PostgreSQL.")
                else:
                    mysql_logger.error(f"Inserimento fallito per il record MySQL {data}: {error_msg}")
                    # Aggiorna lo stato in PostgreSQL come 'ERROR'
                    update_status_query = """
                        UPDATE raw_operazione
                        SET stato = 'ERROR'
                        WHERE id_operazione = %s;
                    """
                    pg_cursor.execute(update_status_query, (record['id_operazione'],))
                    pg_conn.commit()

            except Exception as e:
                mysql_logger.error(f"Errore durante il trasferimento del record {record}: {e}", exc_info=True)
                pg_conn.rollback()
                my_conn.rollback()
                etl_status['last_error'] = str(e)

        postgres_logger.info("Processo di validazione e trasferimento completato con successo.")
        etl_status['last_success'] = datetime.utcnow().isoformat()
        etl_status['last_error'] = None
        return 200

    except Exception as e:
        mysql_logger.error(f'Errore generico nel processo di trasferimento: {e}', exc_info=True)
        periodic_logger.error(f"Processo di trasferimento fallito con errore: {e}")
        etl_status['last_error'] = str(e)
        return 500

    finally:
        etl_status['running'] = False
        periodic_logger.info("Processo di trasferimento completato")
        if pg_cursor:
            pg_cursor.close()
        if pg_conn:
            pg_conn.close()
            postgres_logger.info('Connessione a PostgreSQL chiusa.')
        if my_cursor:
            my_cursor.close()
        if my_conn and my_conn.is_connected():
            my_conn.close()
            mysql_logger.info('Connessione a MySQL chiusa.')

###############################################################################
#                  FUNZIONI PER LA GESTIONE DEGLI ORDINI E PEZZI              #
###############################################################################
def get_pezzo_min_idordine():
    """
    Restituisce i pezzi con l'id_ordine minore e, se non trovati,
    ne crea alcuni fittizi.
    """
    my_conn = None
    my_cursor = None
    response = []

    try:
        my_conn = connect_to_db()
        my_cursor = my_conn.cursor(dictionary=True)
        logging.info('Connesso a MySQL.')

        query = """
            SELECT po.id_ordine, po.id_pezzo
            FROM pezzi_ordine po
            JOIN ordine o ON po.id_ordine = o.id_ordine
            WHERE o.stato = 'IN ATTESA'
              AND po.quantita_rimanente <= po.quantita_totale
              AND po.quantita_rimanente > 0
            ORDER BY po.id_ordine ASC
            LIMIT 5;
        """
        my_cursor.execute(query)
        results = my_cursor.fetchall()
        logging.info(f"Risultati trovati: {results}")

        if results:
            for row in results:
                response.append({
                    "id_ordine": row['id_ordine'],
                    "id_pezzo": row['id_pezzo']
                })
        else:
            # Creo 10 righe fittizie
            for i in range(10):
                response.append({
                    "id_ordine": None,
                    "id_pezzo": i + 1
                })
            update_magazzino_fake(my_cursor, my_conn, response)

    except mysql.connector.Error as err:
        logging.error(f"Errore di connessione al database MySQL: {err}")

    finally:
        # Aggiorno le quantità
        if response:
            logging.info(f"Aggiorno quantità con i seguenti dati: {response}")
            try:
                aggiorna_quantita_pezzi_ordine(response)
            except Exception as e:
                logging.error(f"Errore durante l'aggiornamento della quantità: {e}")
        
        if my_cursor:
            my_cursor.close()
        if my_conn and my_conn.is_connected():
            my_conn.close()
            logging.info("Connessione a MySQL chiusa correttamente.")

    return response

def update_magazzino_fake(cursor, conn, response):
    """
    Aggiorna il magazzino per i pezzi fittizi, incrementandone la quantità di 1.
    """
    query = """
        UPDATE magazzino
        SET quantita_disponibile = quantita_disponibile + 1
        WHERE codice_pezzo = %s;
    """
    for item in response:
        try:
            cursor.execute(query, (item['id_pezzo'],))
            conn.commit()
        except Exception as e:
            logging.error(f"Errore durante l'aggiornamento del pezzo {item['id_pezzo']}: {e}")

def aggiorna_quantita_pezzi_ordine(response):
    """
    Decrementa di 1 la quantita_rimanente per ogni voce presente in 'response'.
    """
    conn = None
    cursor = None
    try:
        conn = connect_to_db()
        cursor = conn.cursor(dictionary=True)
        logging.info('Connesso a MySQL.')

        query = """
            UPDATE defaultdb.pezzi_ordine
            SET quantita_rimanente = quantita_rimanente - 1
            WHERE id_ordine = %s AND id_pezzo = %s;
        """
        for row in response:
            if row['id_ordine'] is not None:
                cursor.execute(query, (row['id_ordine'], row['id_pezzo']))

        conn.commit()
        logging.info('Quantità aggiornata con successo per tutti i pezzi.')
    except mysql.connector.Error as e:
        logging.error(f"Errore durante l'aggiornamento della quantità: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
            logging.info('Connessione a MySQL chiusa.')

def aggiorna_stato_ordini():
    """
    Aggiorna lo stato di tutti gli ordini da 'IN ATTESA' a 'COMPLETATO'
    se tutti i pezzi associati hanno quantita_rimanente = 0.
    """
    conn = None
    cursor = None
    try:
        conn = connect_to_db()
        cursor = conn.cursor(dictionary=True)
        logging.info('Connesso a MySQL.')

        query_ordini = """
            SELECT id_ordine
            FROM ordine
            WHERE stato = 'IN ATTESA' or stato = 'COMPLETATO';
        """
        cursor.execute(query_ordini)
        ordini = cursor.fetchall()
        logging.info(f"Ordini trovati: {ordini}")

        if not ordini:
            logging.info("Non ci sono ordini 'IN ATTESA' o 'COMPLETATO'.")
            return

        for ordine in ordini:
            if 'id_ordine' not in ordine:
                logging.info(f"Chiave 'id_ordine' non presente in: {ordine}")
                continue

            id_ordine = ordine['id_ordine']

            query_pezzi = """
                SELECT quantita_rimanente
                FROM pezzi_ordine
                WHERE id_ordine = %s;
            """
            cursor.execute(query_pezzi, (id_ordine,))
            pezzi = cursor.fetchall()

            if not pezzi:
                logging.info(f"Ordine {id_ordine}: Nessun pezzo associato, imposto stato a COMPLETATO.")
                aggiorna_stato_ordine(cursor, id_ordine)
                continue

            quantita_totale = sum(pezzo['quantita_rimanente'] for pezzo in pezzi)
            if quantita_totale == 0:
                logging.info(f"Ordine {id_ordine}: tutte le quantità sono 0, imposto stato a COMPLETATO.")
                aggiorna_stato_ordine(cursor, id_ordine)

        conn.commit()
    except mysql.connector.Error as err:
        logging.info(f"Errore durante l'aggiornamento: {err}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

def aggiorna_stato_ordine(cursor, id_ordine: int):
    """
    Imposta lo stato dell'ordine a 'COMPLETATO' e aggiorna la data_fine.
    """
    data_fine = date.today().strftime('%Y-%m-%d')
    query_aggiorna_ordine = """
        UPDATE ordine
        SET stato = 'COMPLETATO', data_fine = %s
        WHERE id_ordine = %s;
    """
    cursor.execute(query_aggiorna_ordine, (data_fine, id_ordine))
    logging.info(f"Stato dell'ordine {id_ordine} aggiornato a 'COMPLETATO' con data_fine = {data_fine}.")

###############################################################################
#                       DECORATOR PER PROTEZIONE API KEY                      #
###############################################################################
def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        api_key = request.headers.get('X-API-KEY')
        if not api_key or api_key != os.getenv('API_KEY'):
            logging.warning("Tentativo di accesso non autorizzato.")
            return jsonify({'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    return decorated

###############################################################################
#                              ENDPOINT API FLASK                             #
###############################################################################
@app.route('/run-etl', methods=['POST'])
@require_api_key
def trigger_etl():
    """
    Avvia l'ETL in un thread separato, se non è già in esecuzione.
    """
    if etl_status['running']:
        return jsonify({'status': 'ETL già in esecuzione.'}), 400

    try:
        data = request.get_json()
        if not isinstance(data, list):
            return jsonify({'error': 'I dati devono essere una lista di record.'}), 400

        thread = Thread(target=main_etl_postgres, args=(data,))
        thread.start()

        logging.info('Processo ETL avviato tramite API.')
        return jsonify({'status': 'ETL avviato.'}), 202
    except Exception as e:
        logging.error(f'Errore nell\'avvio dell\'ETL: {e}')
        return jsonify({'error': 'Errore del server interno.'}), 500

@app.route('/insert-postgres', methods=['POST'])
@require_api_key
def insert_postgres_endpoint():
    """
    Endpoint per inserire dati nella tabella raw_operazione tramite ETL PostgreSQL.
    """
    try:
        data = request.get_json()
        postgres_logger.info(f'Dati ricevuti per PostgreSQL: {data}')
        
        if not isinstance(data, list):
            return jsonify({'error': 'I dati devono essere una lista di record.'}), 400

        result = main_etl_postgres(data)
        if result == 200:
            return jsonify({'message': 'Dati inseriti con successo in PostgreSQL.'}), 200
        else:
            return jsonify({'error': 'Inserimento fallito in PostgreSQL.'}), 500
    except Exception as e:
        postgres_logger.error(f'Errore nella gestione della richiesta PostgreSQL: {e}')
        return jsonify({'error': 'Errore del server interno.'}), 500

@app.route('/process-transfer', methods=['POST'])
def process_transfer():
    """
    Endpoint per avviare il processo di validazione e trasferimento dei dati da PostgreSQL a MySQL.
    """
    if etl_status['running']:
        return jsonify({'status': 'Processo di trasferimento già in esecuzione.'}), 400

    try:
        thread = Thread(target=process_and_transfer_to_mysql)
        thread.start()

        mysql_logger.info('Processo di validazione e trasferimento avviato tramite API.')
        return jsonify({'status': 'Processo di trasferimento avviato.'}), 202
    except Exception as e:
        mysql_logger.error(f'Errore nell\'avvio del processo di trasferimento: {e}')
        return jsonify({'error': 'Errore del server interno.'}), 500

@app.route('/ordine', methods=['GET'])
@require_api_key
def get_ordine():
    """
    Restituisce i pezzi con l'id_ordine minore (se esistono) o crea record fittizi.
    """
    try:
        json_data = get_pezzo_min_idordine()
        return jsonify(json_data), 200
    except Exception as e:
        logging.error(f'Errore nella creazione dell\'ordine: {e}')
        return jsonify({'error': 'Impossibile creare ordine.'}), 500

@app.route('/aggiorna/ordine', methods=['POST'])
def aggiorna_ordine():
    """
    Aggiorna lo stato degli ordini: da 'IN ATTESA' a 'COMPLETATO' se terminati.
    """
    try:
        aggiorna_stato_ordini()
        return jsonify({'message': 'Ordine aggiornato con successo'}), 200
    except Exception as e:
        logging.error(f"Errore nell'aggiornamento della tabella ordine: {e}")
        return jsonify({'error': 'Impossibile aggiornare ordine.'}), 500

@app.route('/logs', methods=['GET'])
def get_logs():
    """
    Restituisce il contenuto del file 'etl.log'.
    """
    try:
        with open('logs/etl.log', 'r') as log_file:
            logs = log_file.read()
        return jsonify({'logs': logs}), 200
    except Exception as e:
        logging.error(f'Errore nella lettura del file di log: {e}')
        return jsonify({'error': 'Impossibile leggere i log.'}), 500

@app.route('/mysql-logs', methods=['GET'])
def get_logs_mysql():
    """
    Restituisce il contenuto del file 'etl.log'.
    """
    try:
        with open('logs/mysql.log', 'r') as log_file:
            logs = log_file.read()
        return jsonify({'logs': logs}), 200
    except Exception as e:
        logging.error(f'Errore nella lettura del file di log: {e}')
        return jsonify({'error': 'Impossibile leggere i log.'}), 500

@app.route('/postgresql-logs', methods=['GET'])
def get_logs_postgres():
    """
    Restituisce il contenuto del file 'etl.log'.
    """
    try:
        with open('logs/postgresql.log', 'r') as log_file:
            logs = log_file.read()
        return jsonify({'logs': logs}), 200
    except Exception as e:
        logging.error(f'Errore nella lettura del file di log: {e}')
        return jsonify({'error': 'Impossibile leggere i log.'}), 500

@app.route('/log-cron', methods=['GET'])
def get_log_cron():
    """
    Restituisce il contenuto del file 'periodic.log'.
    """
    try:
        with open('logs/periodic.log', 'r') as log_file:
            logs = log_file.read()
        return jsonify({'logs': logs}), 200
    except Exception as e:
        logging.error(f'Errore nella lettura del file di log: {e}')
        return jsonify({'error': 'Impossibile leggere i log.'}), 500

ALLOWED_LOG_FILES = {
    'etl': 'logs/etl.log',
    'periodic': 'logs/periodic.log',
    'postgresql': 'logs/postgresql.log',
    'mysql': 'logs/mysql.log'
}

@app.route('/clear-logs', methods=['GET', 'POST'])
@require_api_key
def clear_logs():
    """
    Pulisce i file di log specificati. Se nessun parametro è fornito, pulisce tutti i log.
    """
    file_param = request.args.get('file')

    if file_param:
        # Pulizia di un singolo file di log
        log_file = ALLOWED_LOG_FILES.get(file_param.lower())
        if log_file:
            clear_log_file(log_file)
            return jsonify({'message': f"File di log '{file_param}' pulito con successo."}), 200
        else:
            return jsonify({'error': f"File di log '{file_param}' non riconosciuto."}), 400
    else:
        # Pulizia di tutti i file di log
        for log_file in ALLOWED_LOG_FILES.values():
            clear_log_file(log_file)
        return jsonify({'message': 'Tutti i file di log sono stati puliti con successo.'}), 200

@app.route('/status', methods=['GET'])
def get_status():
    """
    Restituisce lo stato corrente dell'ETL.
    """
    return jsonify(etl_status), 200
###############################################################################
#                                  MAIN APP                                   #
###############################################################################
if __name__ == '__main__':
    # Assicurati che la directory dei log esista
    if not os.path.exists('logs'):
        os.makedirs('logs')

    app.run(host='0.0.0.0', port=5000, debug=True)
