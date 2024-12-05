import os
import psycopg2
import mysql.connector
import logging
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from threading import Thread
import math
import shutil
from datetime import datetime, timezone, timedelta
from flask_cors import CORS

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

data_processed = {
    'data':[]
}

data_forgiatura = {
    'data':[]
}

data_anomali = {
    'data':[]
}

# PostgreSQL connection parameters
PG_HOST = os.getenv('PG_HOST')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')

# MySQL connection parameters
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = os.getenv('MYSQL_PORT')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')

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

def parse_timestamp(timestamp_str):
    try:
        # Try parsing with microseconds
        try:
            parsed_timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')
            return parsed_timestamp
        except ValueError:
            # If microseconds parsing fails, try without microseconds
            parsed_timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            return parsed_timestamp
    except ValueError as e:
        logging.error(f"Unable to parse timestamp: {timestamp_str}")
        raise

def validate_timestamp(timestamp_str, tolerance_minutes=60):
    try:
        parsed_timestamp = parse_timestamp(timestamp_str)
        now = datetime.utcnow()
        
        # Increased tolerance to 60 minutes
        tolerance = timedelta(minutes=tolerance_minutes)

        # Debug logging
        print(f"Parsed Timestamp: {parsed_timestamp}")
        print(f"Current UTC Time: {now}")
        print(f"Timestamp diff from now: {parsed_timestamp - now}")

        # Check if timestamp is within tolerance of current time
        if abs(parsed_timestamp - now) > tolerance:
            print(f"Timestamp is more than {tolerance_minutes} minutes from current time!")
            return False
        return True
    except Exception as e:
        print(f"Timestamp validation error: {e}")
        return False

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
    my_cursor.execute("SELECT * FROM Forgiatura")
    data_forgiatura = my_cursor.fetchall()
    colnames_forgiatura = [desc[0] for desc in my_cursor.description]

    # Dati dalla tabella dati_anomali
    my_cursor.execute("SELECT * FROM dati_anomali")
    data_anomali = my_cursor.fetchall()
    colnames_anomali = [desc[0] for desc in my_cursor.description]

    # Chiusura connessione
    if 'my_cursor' in locals() and my_cursor:
        my_cursor.close()
    if 'my_conn' in locals() and my_conn.is_connected():
        my_conn.close()

    # Ritorno dei dati e delle colonne
    return (data_forgiatura, colnames_forgiatura), (data_anomali, colnames_anomali)

def run_etl():
    global etl_status
    etl_status['running'] = True
    etl_status['last_run'] = datetime.utcnow().isoformat()

    try:
        periodic_logger.info('Operazione periodica avviata.')
        # Connessione a PostgreSQL
        pg_conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        pg_cursor = pg_conn.cursor()
        logging.info('Connesso a PostgreSQL.')

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

        # Estrazione dei dati dalla tabella staging
        pg_cursor.execute("SELECT * FROM RawForgiatura")
        raw_data = pg_cursor.fetchall()
        colnames = [desc[0] for desc in pg_cursor.description]
        logging.info(f'Estrazione di {len(raw_data)} record da PostgreSQL.')

        # Registrazione dei risultati
        periodic_logger.info(f'Estrazione di {len(raw_data)} record da PostgreSQL.')

        # Carica gli ID macchina validi in memoria per confronto
        # my_cursor.execute("SELECT codice_macchinario FROM Macchinari")
        # valid_machine_ids = {row[0] for row in pg_cursor.fetchall()}

        transformed_data = []
        invalid_records = []  # Per salvare i record con anomalie

        for row in raw_data:
            data = dict(zip(colnames, row))
            try:
                # Escludere la colonna "anomalia" dalla validazione
                columns_to_check = {key: value for key, value in data.items() if key != 'anomalia'}

                tipo_anomalia = []  # Lista per raccogliere le anomalie del record

                # Regola: Valori mancanti
                for key, value in columns_to_check.items():
                    if value is None or (isinstance(value, float) and math.isnan(value)):
                        tipo_anomalia.append(f"{key} mancante")

                # Regola: Valore fuori range
                if 'peso_effettivo' in data and (data['peso_effettivo'] < 0 or data['peso_effettivo'] > 1000):
                    tipo_anomalia.append("Peso fuori range")

                if 'temperatura_effettiva' in data and (data['temperatura_effettiva'] < -50 or data['temperatura_effettiva'] > 150):
                    tipo_anomalia.append("Temperatura fuori range")

                # Regola: ID macchina non valido
                # if 'id_macchina' in data and data['id_macchina'] not in valid_machine_ids:
                    # tipo_anomalia.append("ID macchina non valido")
                
                try:
                    logging.info(f"Timestamp originale: {data['timestamp_ricevuto']}")

                    # Parsing e validazione del timestamp
                    if isinstance(data['timestamp_ricevuto'], datetime):
                        parsed_timestamp = data['timestamp_ricevuto']
                        is_valid_timestamp = True
                    else:
                        is_valid_timestamp, parsed_timestamp = validate_timestamp(data['timestamp_ricevuto'])

                    # Se il timestamp non è valido, aggiungi l'anomalia
                    if not is_valid_timestamp:
                        tipo_anomalia.append("Timestamp ricevuto non valido o fuori intervallo")

                    # Salva il timestamp parsato nel dizionario
                    data['timestamp_ricevuto'] = parsed_timestamp

                    logging.info(f"Timestamp parsato: {data['timestamp_ricevuto']}, Ora attuale (UTC): {datetime.utcnow()}")
                except (ValueError, TypeError) as e:
                    logging.error(f"Errore nel parsing del timestamp '{data['timestamp_ricevuto']}': {e}")
                    tipo_anomalia.append("Timestamp ricevuto non valido")

                # Se ci sono anomalie, salva il record nei dati invalidi
                if tipo_anomalia:
                    data['tipo_anomalia'] = "; ".join(tipo_anomalia)
                    logging.warning(f"Record con anomalie rilevate: {data['tipo_anomalia']} - {data}")
                    invalid_records.append(data)
                    continue

                # Trasforma i dati validi
                transformed_record = (
                    data['id_pezzo'],
                    data['peso_effettivo'],
                    data['temperatura_effettiva'],
                    data['timestamp_ricevuto'],
                    data['id_macchina']
                )
                transformed_data.append(transformed_record)
            except KeyError as e:
                record_id = data.get('id', 'N/A')
                logging.error(f'Errore nella trasformazione dei dati per il record {record_id}: chiave mancante {e}')
                continue

        logging.info('Trasformazione dei dati completata.')

        # Log dei record invalidi
        if invalid_records:
            logging.warning(f'Trovati {len(invalid_records)} record con valori NaN/NULL.')

        mysql_insert_success = False

        # Trasformazione dei record non validi
        invalid_records_tuples = [
            (
                record['id_pezzo'],
                record['peso_effettivo'],
                record['temperatura_effettiva'],
                record['timestamp_ricevuto'],
                record['id_macchina'],
                record['tipo_anomalia']
            )
            for record in invalid_records
        ]

        # Inserimento dei dati validi
        try:
            disable_foreign_keys(my_cursor)
            insert_query_valid = """
            INSERT INTO Forgiatura (codice_pezzo, peso_effettivo, temperatura_effettiva, timestamp, codice_macchinario)
            VALUES (%s, %s, %s, %s, %s)
            """
            if save_records(my_cursor, my_conn, insert_query_valid, transformed_data):
                mysql_insert_success = True
                periodic_logger.info(f"Salvati {len(transformed_data)} record nella tabella Forgiatura in MySQL")
            else:
                etl_status['last_error'] = 'Errore durante l\'inserimento dei dati validi in MySQL.'
        except Exception as e:
            logging.error(f'Errore durante il salvataggio dei dati validi: {e}')
        finally:
            enable_foreign_keys(my_cursor)

        # Inserimento dei dati non validi
        try:
            disable_foreign_keys(my_cursor)
            insert_query_invalid = """
            INSERT INTO dati_anomali (codice_pezzo, peso_effettivo, temperatura_effettiva, timestamp, codice_macchinario, tipo_anomalia)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            if save_records(my_cursor, my_conn, insert_query_invalid, invalid_records_tuples):
                mysql_insert_success = True
                periodic_logger.info(f"Salvati {len(invalid_records_tuples)} record nella tabella dati_anomali in MySQL")
            else:
                etl_status['last_error'] = 'Errore durante l\'inserimento dei dati non validi in MySQL.'
                mysql_insert_success = False
        except Exception as e:
            logging.error(f'Errore durante il salvataggio dei dati non validi: {e}')
            etl_status['last_error'] = str(e)
        finally:
            enable_foreign_keys(my_cursor)

        if mysql_insert_success:
            try:
                # Eliminazione dei dati solo se l'inserimento ha avuto successo
                delete_query = """
                DELETE FROM RawForgiatura
                WHERE id = ANY(%s)
                """
                pg_cursor.execute(delete_query, ([row[0] for row in raw_data],))
                pg_conn.commit()
                logging.info(f'Eliminati {pg_cursor.rowcount} record processati dal database di staging.')

                etl_status['last_success'] = datetime.utcnow().isoformat()
                etl_status['last_error'] = None
            except psycopg2.Error as e:
                logging.error(f'Errore durante l\'eliminazione dei dati in Postgres: {e}')
                etl_status['last_error'] = f'Errore durante l\'eliminazione in PostgreSQL: {e}'
                pg_conn.rollback()

    except Exception as e:
        logging.error(f'Errore generico: {e}')
        periodic_logger.error(f"ETL fallito con errore: {e}")
        etl_status['last_error'] = str(e)

    finally:
        etl_status['running'] = False
        periodic_logger.info(f"ETL completato con successo")

        # Chiusura delle connessioni
        if 'my_cursor' in locals() and my_cursor:
            my_cursor.close()
        if 'my_conn' in locals() and my_conn.is_connected():
            my_conn.close()
            logging.info('Connessione a MySQL chiusa.')
        if 'pg_cursor' in locals() and pg_cursor:
            pg_cursor.close()
        if 'pg_conn' in locals() and pg_conn:
            pg_conn.close()
            logging.info('Connessione a PostgreSQL chiusa.')

@app.route('/run-etl', methods=['POST'])
def trigger_etl():
    if etl_status['running']:
        return jsonify({'status': 'ETL già in esecuzione.'}), 400

    # Avvia l'ETL in un thread separato per evitare il blocco del server
    thread = Thread(target=run_etl)
    thread.start()

    logging.info('Processo ETL avviato tramite API.')
    return jsonify({'status': 'ETL avviato.'}), 202

@app.route('/status', methods=['GET'])
def get_status():
    return jsonify(etl_status), 200

@app.route('/run', methods=['GET'])
def run():
    run_etl()
    return jsonify(etl_status, data_processed), 200

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
def clear_logs():
    log_file_path = 'logs/etl.log'  # Percorso del file di log
    clear_log_file(log_file_path)
    log_file_path2 = 'logs/periodic.log'  # Percorso del file di log
    clear_log_file(log_file_path2)
    return jsonify({'message': 'File di log pulito con successo.'}), 200

@app.route('/processed-data', methods=['GET'])
def get_processed_data():
    if not data_processed['data']:
        return jsonify({'data': [], 'message': 'Nessun dato processato.'}), 200
    return jsonify(data_processed), 200

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