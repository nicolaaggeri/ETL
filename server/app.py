import os
import mysql.connector
import logging
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from threading import Thread
import math
import shutil
from datetime import datetime, timezone, timedelta
from flask_cors import CORS
from typing import List, Optional
from pydantic import BaseModel, Field, ValidationError, validator
from functools import wraps
from mysql.connector import Error

# Configure logging
logging.basicConfig(
    filename='logs/etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configura il logger per attività periodiche
periodic_logger = logging.getLogger('periodic_logger')
periodic_logger.setLevel(logging.INFO)

# Configura un handler per il file di log delle operazioni periodiche
periodic_handler = logging.FileHandler('logs/periodic.log')
periodic_handler.setLevel(logging.INFO)

# Configura un formatter per il logger periodico
periodic_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
periodic_handler.setFormatter(periodic_formatter)

# Aggiungi il handler al logger periodico
periodic_logger.addHandler(periodic_handler)

# Load environment variables from .env file
load_dotenv(dotenv_path='config/.env')

# Inizializzazione dell'app Flask
app = Flask(__name__)

CORS(app)

# Variabile globale per tracciare lo stato dell'ETL
etl_status = {
    'running': False,
    'last_run': None,
    'last_success': None,
    'last_error': None
}

FIELD_TO_ANOMALIA_ID = {
    'timestamp_fine': 1,
    'peso_effettivo': 2,
    'temperatura_effettiva': 3,
    'anomalia': 4
    # Aggiungi altri campi se necessario
}

data_processed = {
    'data':[]
}

# MySQL connection parameters
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = os.getenv('MYSQL_PORT')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')

class Anomalia(BaseModel):
    id: int

class Operazione(BaseModel):
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
        if values.get('tipo_operazione') == 'forgiatura':
            if v is None or not (0 <= v <= 1000):
                raise ValueError('Peso fuori range')
        return v

    @validator('temperatura_effettiva')
    def temperatura_effettiva_range(cls, v, values):
        if values.get('tipo_operazione') == 'forgiatura':
            if v is None or not (-50 <= v <= 150):
                raise ValueError('Temperatura fuori range')
        return v

    @validator('timestamp_inizio', 'timestamp_fine', pre=True)
    def validate_and_parse_timestamp(cls, v, field):
        """
        Valida e parsifica i timestamp utilizzando le funzioni personalizzate.
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

def clear_log_file(log_file_path, backup=True):
    """
    Pulisce il file di log, mantenendo opzionalmente una copia di backup.

    :param log_file_path: Percorso del file di log da pulire.
    :param backup: Se True, crea una copia di backup del file prima di svuotarlo.
    """
    try:
        if backup:
            backup_path = f"{log_file_path}.backup"
            shutil.copy(log_file_path, backup_path)
            logging.info(f"Backup del file di log creato: {backup_path}")

        # Svuota il file
        with open(log_file_path, 'w') as log_file:
            log_file.truncate(0)
            logging.info(f"File di log '{log_file_path}' svuotato con successo.")

    except FileNotFoundError:
        logging.warning(f"Il file di log '{log_file_path}' non è stato trovato.")
    except Exception as e:
        logging.error(f"Errore durante la pulizia del file di log '{log_file_path}': {e}")

def disable_foreign_keys(cursor):
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
    logging.info("Foreign key checks disabilitati.")

def enable_foreign_keys(cursor):
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
    logging.info("Foreign key checks riabilitati.")

def parse_timestamp(timestamp_str: str) -> datetime:
    """
    Tenta di parsare una stringa timestamp con o senza microsecondi.
    """
    try:
        # Prova a parsare con microsecondi
        try:
            parsed_timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')
            return parsed_timestamp
        except ValueError:
            # Se fallisce, prova senza microsecondi
            parsed_timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            return parsed_timestamp
    except ValueError as e:
        logging.error(f"Impossibile parsare il timestamp: {timestamp_str}")
        raise

def validate_timestamp(timestamp_str: str, tolerance_minutes: int = 60) -> datetime:
    """
    Valida che il timestamp sia entro una certa tolleranza rispetto all'orario corrente.
    Restituisce l'oggetto datetime se valido, altrimenti solleva un'eccezione.
    """
    try:
        parsed_timestamp = parse_timestamp(timestamp_str)
        now = datetime.utcnow()
        
        # Aumentata tolleranza a 60 minuti
        tolerance = timedelta(minutes=tolerance_minutes)

        # Logging di debug
        logging.debug(f"Parsed Timestamp: {parsed_timestamp}")
        logging.debug(f"Current UTC Time: {now}")
        logging.debug(f"Timestamp diff from now: {parsed_timestamp - now}")

        # Verifica se il timestamp è entro la tolleranza rispetto all'orario corrente
        if parsed_timestamp > now + tolerance:
            raise ValueError(f"Timestamp fuori dalla tolleranza di {tolerance_minutes} minuti")
        
        return parsed_timestamp
    except Exception as e:
        logging.error(f"Errore nella validazione del timestamp: {e}")
        raise

def connect_to_db():
    """Crea e ritorna una connessione al database MySQL."""
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
        logging.error(f"Errore nella connessione al database: {e}")
        raise

def insert_operation_data(cursor, data: dict) -> (bool, Optional[str], Optional[int]):
    try:
        # Inserimento operazione
        insert_operazione = """
        INSERT INTO operazioni (id_ordine, codice_pezzo, codice_macchinario, codice_operatore, timestamp_inizio, timestamp_fine)
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

        # Inserimento dettagli in base al tipo di operazione
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
            # Tipo operazione sconosciuto
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

        return True, None, id_operazione

    except Exception as e:
        logging.error(f'Errore durante l\'inserimento nel database: {e}', exc_info=True)
        return False, str(e), None

def main_etl(rows: List[dict]) -> int:
    global etl_status
    etl_status['running'] = True
    etl_status['last_run'] = datetime.utcnow().isoformat()

    my_conn = None
    my_cursor = None

    try:
        my_conn = connect_to_db()
        my_cursor = my_conn.cursor(dictionary=True)
        logging.info('Connesso a MySQL.')

        disable_foreign_keys(my_cursor)

        for i, data in enumerate(rows):
            logging.debug(f"Processo record #{i}: {data}")
            try:
                operazione = Operazione(**data)
                operazione_dict = operazione.dict()
                success, error_msg, id_operazione = insert_operation_data(my_cursor, operazione_dict)
                if success:
                    logging.info(f"Record inserito con successo: ID {id_operazione}")

                    # Sottrai 1 dalla colonna 'quantita' per il pezzo corrispondente
                    id_ordine = data.get('id_ordine')
                    codice_pezzo = data.get('codice_pezzo')
                    
                    if id_ordine and codice_pezzo:
                        update_quantita_query = """
                        UPDATE pezzi_ordine
                        SET quantita = quantita - 1
                        WHERE id_ordine = %s AND id_pezzo = %s AND quantita > 0;
                        """
                        my_cursor.execute(update_quantita_query, (id_ordine, codice_pezzo))
                        logging.info(f"Quantità decrementata per id_ordine={id_ordine}, codice_pezzo={codice_pezzo}")

                else:
                    logging.error(f"Inserimento fallito per il record {data}: {error_msg}")

            except ValidationError as ve:
                errors = ve.errors()
                logging.warning(f"Record invalido: {data} - Anomalie: {errors}")

                anomalie = []
                operazione_dict = data.copy()
                for error in errors:
                    field = error['loc'][-1]
                    message = error['msg']
                    anomaly_id = FIELD_TO_ANOMALIA_ID.get(field, 999)
                    anomalie.append({'id': anomaly_id, 'message': f"{field}: {message}"})
                    operazione_dict[field] = None

                try:
                    success, error_msg, id_operazione = insert_operation_data(my_cursor, operazione_dict)
                    if success:
                        logging.info(f"Record inserito con campi invalidi: ID {id_operazione}")
                        if anomalie and id_operazione:
                            insert_anomalia_operazione = """
                            INSERT INTO anomalia_operazione (id_anomalia, id_operazione, note)
                            VALUES (%s, %s, %s)
                            """
                            for anomaly in anomalie:
                                anomaly_id = anomaly.get('id')
                                message = anomaly.get('message')
                                my_cursor.execute(insert_anomalia_operazione, (anomaly_id, id_operazione, message))
                    else:
                        logging.error(f"Inserimento fallito per il record con anomalie {data}: {error_msg}")
                except Exception as e:
                    logging.error(f"Errore durante l'inserimento del record invalidato {data}: {e}", exc_info=True)

            except Exception as e:
                logging.error(f"Errore durante il processamento del record {data}: {e}", exc_info=True)
                # In caso di errore generale, effettua rollback, riabilita le foreign key e aggiorna lo stato
                my_conn.rollback()
                enable_foreign_keys(my_cursor)
                etl_status['last_error'] = str(e)
                return 500

        # Se tutto è andato a buon fine
        my_conn.commit()
        enable_foreign_keys(my_cursor)
        logging.info("ETL completato con successo.")
        etl_status['last_success'] = datetime.utcnow().isoformat()
        etl_status['last_error'] = None
        return 200

    except Exception as e:
        logging.error(f'Errore generico ETL: {e}', exc_info=True)
        periodic_logger.error(f"ETL fallito con errore: {e}")
        etl_status['last_error'] = str(e)
        return 500

    finally:
        etl_status['running'] = False
        periodic_logger.info("ETL completato")
        if my_cursor:
            my_cursor.close()
        if my_conn and my_conn.is_connected():
            my_conn.close()
            logging.info('Connessione a MySQL chiusa.')

def save_records(cursor, connection, query, records, retries=3):
    """Funzione generica per salvare record nel database con retry."""
    logging.info(f"Query: {query}")
    logging.info(f"Esempio di record: {records[:1]}")  # Mostra il primo record

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

def show_tables():
    # Connessione a MySQL
    my_conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DATABASE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )
    my_cursor = my_conn.cursor()
    logging.info('Connesso a MySQL.')

    # Dati dalla tabella Forgiatura
    my_cursor.execute("SELECT * FROM forgiatura")
    data_forgiatura = my_cursor.fetchall()
    colnames_forgiatura = [desc[0] for desc in my_cursor.description]

    # Dati dalla tabella dati_anomali
    my_cursor.execute("SELECT * FROM cnc")
    data_cnc = my_cursor.fetchall()
    colnames_anomali = [desc[0] for desc in my_cursor.description]

    # Chiusura connessione
    if 'my_cursor' in locals() and my_cursor:
        my_cursor.close()
    if 'my_conn' in locals() and my_conn.is_connected():
        my_conn.close()

    # Ritorno dei dati e delle colonne
    return (data_forgiatura, colnames_forgiatura), (data_cnc, colnames_anomali)

    import mysql.connector
import json

def get_pezzo_min_idordine():
    try:
        # Connessione al database
        my_conn = connect_to_db()
        my_cursor = my_conn.cursor(dictionary=True)
        logging.info('Connesso a MySQL.')

        # Query per selezionare i pezzi con l'id_ordine minore
        query = """
        SELECT po.id_ordine, po.id_pezzo
        FROM pezzi_ordine po
        JOIN ordine o ON po.id_ordine = o.id_ordine
        WHERE o.stato = 'IN ATTESA' 
        AND po.quantita != 0
        ORDER BY po.id_ordine ASC
        LIMIT 5;
        """
        
        my_cursor.execute(query)
        results = my_cursor.fetchall()  # Ottieni tutte le righe

        logging.info(results)

        # Composizione del JSON
        response = []
        for row in results:
            response.append({
                "id_ordine": row['id_ordine'],
                "id_pezzo": row['id_pezzo']
            })
        
        # Se non ci sono risultati, aggiungi 10 righe con id_ordine: None e id_pezzo da 0 a 9
        if not results:
            for i in range(10):
                response.append({
                    "id_ordine": None,
                    "id_pezzo": i+1
                })

        return response

    except mysql.connector.Error as err:
        logging.error(f"Errore: {err}")
        return []

    finally:
        if my_conn.is_connected():
            my_cursor.close()
            my_conn.close()
            logging.info("Connessione chiusa.")

def aggiorna_stato_ordini():
    try:
        conn = connect_to_db()
        cursor = conn.cursor(dictionary=True)
        logging.info('Connesso a MySQL.')

        # 1️ Seleziona tutti gli ordini con stato 'IN ATTESA'
        query_ordini = """
        SELECT id_ordine 
        FROM ordine 
        WHERE stato = 'IN ATTESA';
        """
        cursor.execute(query_ordini)
        ordini = cursor.fetchall()

        logging.info(f"Ordini trovati: {ordini}")

        if not ordini:
            logging.info("Non ci sono ordini 'IN ATTESA'.")
            return

        for ordine in ordini:
            if 'id_ordine' not in ordine:
                logging.info(f"Errore: la chiave 'id_ordine' non esiste nell'ordine: {ordine}")
                continue

            id_ordine = ordine['id_ordine']

            # 2️ Seleziona i pezzi associati a questo ordine
            query_pezzi = """
            SELECT quantita 
            FROM pezzi_ordine 
            WHERE id_ordine = %s;
            """
            cursor.execute(query_pezzi, (id_ordine,))
            pezzi = cursor.fetchall()

            if not pezzi:
                # Se non ci sono pezzi associati, setta l'ordine a TERMINATO
                logging.info(f"Ordine {id_ordine}: Nessun pezzo associato. Imposto lo stato a TERMINATO.")
                aggiorna_stato_ordine(cursor, id_ordine)
                continue

            # 3️ Verifica se tutti i pezzi hanno quantita = 0
            quantita_totale = sum(pezzo['quantita'] for pezzo in pezzi)
            
            if quantita_totale == 0:
                # Se tutti i pezzi hanno quantità 0, imposta l'ordine a TERMINATO
                logging.info(f"Ordine {id_ordine}: Tutte le quantità sono 0. Imposto lo stato a TERMINATO.")
                aggiorna_stato_ordine(cursor, id_ordine)

            # 4 Elimina tutti i pezzi con quantità = 0
            elimina_pezzi_quantita_zero(cursor, id_ordine)
        
        # Effettua il commit per salvare le modifiche
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

def aggiorna_stato_ordine(cursor, id_ordine):
    """
    Imposta lo stato dell'ordine a 'TERMINATO' e aggiorna la data_fine con la data di oggi.
    """
    data_fine = date.today().strftime('%Y-%m-%d')
    query_aggiorna_ordine = """
    UPDATE ordine 
    SET stato = 'TERMINATO', data_fine = %s 
    WHERE id_ordine = %s;
    """
    cursor.execute(query_aggiorna_ordine, (data_fine, id_ordine))
    logging.info(f"Stato dell'ordine {id_ordine} aggiornato a 'TERMINATO' con data_fine = {data_fine}.")

def elimina_pezzi_quantita_zero(cursor, id_ordine):
    """
    Elimina dalla tabella pezzi_ordine tutti i pezzi con quantità = 0.
    """
    query_elimina_pezzi = """
    DELETE FROM pezzi_ordine 
    WHERE id_ordine = %s AND quantita = 0;
    """
    cursor.execute(query_elimina_pezzi, (id_ordine,))
    logging.info(f"Pezzi con quantità 0 eliminati per l'ordine {id_ordine}.")

def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        api_key = request.headers.get('X-API-KEY')
        if not api_key or api_key != os.getenv('API_KEY'):
            logging.warning("Tentativo di accesso non autorizzato.")
            return jsonify({'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    return decorated

@app.route('/run-etl', methods=['POST'])
@require_api_key
def trigger_etl():
    if etl_status['running']:
        return jsonify({'status': 'ETL già in esecuzione.'}), 400

    try:
        data = request.get_json()
        if not isinstance(data, list):
            return jsonify({'error': 'I dati devono essere una lista di record.'}), 400

        # Avvia l'ETL in un thread separato
        thread = Thread(target=main_etl, args=(data,))
        thread.start()

        logging.info('Processo ETL avviato tramite API.')
        return jsonify({'status': 'ETL avviato.'}), 202

    except Exception as e:
        logging.error(f'Errore nell\'avvio dell\'ETL: {e}')
        return jsonify({'error': 'Errore del server interno.'}), 500

@app.route('/aggiorna/ordine', methods=['POST'])
def aggiorna_ordine():
    try:
        aggiorna_stato_ordini()
        return jsonify({'message': 'Ordine aggiornato con successo'}), 200
    except Exception as e:
        logging.error(f"Errore nell'aggiornamento della tabella ordine: {e}")
        return jsonify({'error': 'Impossibile aggiornare ordine.'}), 500

@app.route('/status', methods=['GET'])
def get_status():
    return jsonify(etl_status), 200

@app.route('/data', methods=['GET'])
def data():
    try:
        (data_forgiatura, colnames_forgiatura), (data_anomali, colnames_anomali) = show_tables()

        # Prepara i dati in formato leggibile per JSON
        result = {
            "Forgiatura": {
                "columns": colnames_forgiatura,
                "rows": data_forgiatura
            },
            "DatiAnomali": {
                "columns": colnames_anomali,
                "rows": data_anomali
            }
        }
        return jsonify(result), 200
    except Exception as e:
        logging.error(f"Errore durante la lettura dei dati: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/clear-logs', methods=['GET', 'POST'])
@require_api_key
def clear_logs():
    log_file_path = 'logs/etl.log'  # Percorso del file di log
    clear_log_file(log_file_path)
    log_file_path2 = 'logs/periodic.log'  # Percorso del file di log
    clear_log_file(log_file_path2)
    return jsonify({'message': 'File di log pulito con successo.'}), 200

@app.route('/insert', methods=['POST'])
@require_api_key
def insert_endpoint():
    """Endpoint per inserire dati nella tabella Forgiatura."""
    try:
        # Ottieni i dati dal body della richiesta (POST)
        data = request.get_json()
        logging.info(f'Dati ricevuti: {data}')
        
        if not isinstance(data, list):
            return jsonify({'error': 'I dati devono essere una lista di record.'}), 400

        # Chiama la funzione di inserimento
        result = main_etl(data)
        
        if result == 200:
            return jsonify({'message': 'Dati inseriti con successo.'}), 200
        else:
            return jsonify({'error': 'Inserimento fallito.'}), 500

    except Exception as e:
        logging.error(f'Errore nella gestione della richiesta: {e}')
        return jsonify({'error': 'Errore del server interno.'}), 500

@app.route('/ordine', methods=['GET'])
@require_api_key
def get_ordine():
    try:
        json = get_pezzo_min_idordine()
        return jsonify(json), 200
    except Exception as e:
        logging.error(f'Errore nella creazione dell\'ordine: {e}')
        return jsonify({'error': 'Impossibile creare ordine.'}), 500

@app.route('/logs', methods=['GET'])
def get_logs():
    try:
        with open('logs/etl.log', 'r') as log_file:
            logs = log_file.read()
        return jsonify({'logs': logs}), 200
    except Exception as e:
        logging.error(f'Errore nella lettura del file di log: {e}')
        return jsonify({'error': 'Impossibile leggere i log.'}), 500

@app.route('/log-cron', methods=['GET'])
def get_log_cron():
    try:
        with open('logs/periodic.log', 'r') as log_file:
            logs = log_file.read()
        return jsonify({'logs': logs}), 200
    except Exception as e:
        logging.error(f'Errore nella lettura del file di log: {e}')
        return jsonify({'error': 'Impossibile leggere i log.'}), 500

if __name__ == '__main__':
    # Assicurati che la directory dei log esista
    if not os.path.exists('logs'):
        os.makedirs('logs')

    app.run(host='0.0.0.0', port=5000, debug=True)