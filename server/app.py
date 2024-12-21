import os
import math
import json
import shutil
import logging
import mysql.connector

from datetime import datetime, timedelta, date
from threading import Thread
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask_cors import CORS
from functools import wraps
from mysql.connector import Error

# Import dal nostro package
from logging_config import configure_logging
from models import Anomalia, Operazione, FIELD_TO_ANOMALIA_ID

###############################################################################
#                           CONFIGURAZIONE LOGGING                            #
###############################################################################
periodic_logger = configure_logging()

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

###############################################################################
#                         FUNZIONI DI UTILITÀ (LOG E TIMESTAMP)               #
###############################################################################
def clear_log_file(log_file_path: str, backup: bool = True) -> None:
    """
    Pulisce il file di log, mantenendo opzionalmente una copia di backup.
    """
    try:
        if backup:
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

def validate_timestamp(timestamp_str: str, tolerance_minutes: int = 60) -> datetime:
    """
    Valida che il timestamp sia entro una certa tolleranza rispetto all'orario corrente.
    """
    parsed_timestamp = parse_timestamp(timestamp_str)
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
        logging.error(f"Errore nella connessione al database: {e}")
        raise

def insert_operation_data(cursor, data: dict) -> (bool, str, int):
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

        return True, None, id_operazione

    except Exception as e:
        logging.error(f"Errore durante l'inserimento nel database: {e}", exc_info=True)
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
#                          FUNZIONE PRINCIPALE DI ETL                         #
###############################################################################
def main_etl(rows):
    global etl_status
    etl_status['running'] = True
    etl_status['last_run'] = datetime.utcnow().isoformat()

    my_conn = None
    my_cursor = None

    try:
        my_conn = connect_to_db()
        my_cursor = my_conn.cursor(dictionary=True)
        logging.info('Connesso a MySQL.')

        toggle_foreign_keys(my_cursor, enable=False)

        for i, data in enumerate(rows):
            logging.debug(f"Processo record #{i}: {data}")
            try:
                # Validazione con Pydantic
                operazione = Operazione(**data)
                operazione_dict = operazione.dict()

                success, error_msg, id_operazione = insert_operation_data(my_cursor, operazione_dict)
                if success:
                    logging.info(f"Record inserito con successo: ID {id_operazione}")
                    id_ordine = data.get('id_ordine')
                    codice_pezzo = data.get('codice_pezzo')
                    if id_ordine and codice_pezzo:
                        decrement_quantita_pezzo_ordine(my_cursor, id_ordine, codice_pezzo)
                else:
                    logging.error(f"Inserimento fallito per il record {data}: {error_msg}")

            except ValidationError as ve:
                # Gestione errori di validazione: segnalare anomalia
                errors = ve.errors()
                logging.warning(f"Record invalido: {data} - Anomalie: {errors}")

                anomalie = []
                operazione_dict = data.copy()
                for error in errors:
                    field = error['loc'][-1]
                    message = error['msg']
                    anomaly_id = FIELD_TO_ANOMALIA_ID.get(field, 999)
                    anomalie.append({'id': anomaly_id, 'message': f"{field}: {message}"})
                    # Svuota il campo per evitare errori in DB
                    operazione_dict[field] = None

                # Inserisco comunque il record con i campi invalidi forzati a None
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
                                my_cursor.execute(insert_anomalia_operazione,
                                                  (anomaly_id, id_operazione, message))
                    else:
                        logging.error(f"Inserimento fallito per il record con anomalie {data}: {error_msg}")
                except Exception as e:
                    logging.error(f"Errore durante l'inserimento del record invalidato {data}: {e}", exc_info=True)

            except Exception as e:
                logging.error(f"Errore durante il processamento del record {data}: {e}", exc_info=True)
                my_conn.rollback()
                toggle_foreign_keys(my_cursor, enable=True)
                etl_status['last_error'] = str(e)
                return 500

        my_conn.commit()
        toggle_foreign_keys(my_cursor, enable=True)

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
        logging.error(f"Errore di connessione al database: {err}")

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
            logging.info("Connessione chiusa correttamente.")

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
            logging.info('Connessione al database chiusa.')

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
#                              ENDOPINT API FLASK                             #
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

        thread = Thread(target=main_etl, args=(data,))
        thread.start()

        logging.info('Processo ETL avviato tramite API.')
        return jsonify({'status': 'ETL avviato.'}), 202
    except Exception as e:
        logging.error(f'Errore nell\'avvio dell\'ETL: {e}')
        return jsonify({'error': 'Errore del server interno.'}), 500

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

@app.route('/status', methods=['GET'])
def get_status():
    """
    Restituisce lo stato corrente dell'ETL.
    """
    return jsonify(etl_status), 200

@app.route('/clear-logs', methods=['GET', 'POST'])
@require_api_key
def clear_logs():
    """
    Pulisce i file di log 'etl.log' e 'periodic.log'.
    """
    clear_log_file('logs/etl.log')
    clear_log_file('logs/periodic.log')
    return jsonify({'message': 'File di log puliti con successo.'}), 200

@app.route('/insert', methods=['POST'])
@require_api_key
def insert_endpoint():
    """
    Endpoint per inserire dati nei processi di forgiatura/cnc tramite ETL.
    """
    try:
        data = request.get_json()
        logging.info(f'Dati ricevuti: {data}')
        
        if not isinstance(data, list):
            return jsonify({'error': 'I dati devono essere una lista di record.'}), 400

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
    """
    Restituisce i pezzi con l'id_ordine minore (se esistono) o crea record fittizi.
    """
    try:
        json_data = get_pezzo_min_idordine()
        return jsonify(json_data), 200
    except Exception as e:
        logging.error(f'Errore nella creazione dell\'ordine: {e}')
        return jsonify({'error': 'Impossibile creare ordine.'}), 500

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

###############################################################################
#                                  MAIN APP                                   #
###############################################################################
if __name__ == '__main__':
    # Assicurati che la directory dei log esista
    if not os.path.exists('logs'):
        os.makedirs('logs')

    app.run(host='0.0.0.0', port=5000, debug=True)