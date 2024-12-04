import os
import psycopg2
import mysql.connector
import logging
from dotenv import load_dotenv
from datetime import datetime
from flask import Flask, jsonify, request
from threading import Thread
import math

# Configure logging
logging.basicConfig(
    filename='logs/etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load environment variables from .env file
load_dotenv(dotenv_path='config/.env')

# Inizializzazione dell'app Flask
app = Flask(__name__)

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

def disable_foreign_keys(cursor):
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
    logging.info("Foreign key checks disabilitati.")

def enable_foreign_keys(cursor):
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
    logging.info("Foreign key checks riabilitati.")

def save_records(cursor, connection, query, records, retries=3):
    """Funzione generica per salvare record nel database con retry."""
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

def run_etl():
    global etl_status
    etl_status['running'] = True
    etl_status['last_run'] = datetime.utcnow().isoformat()

    try:
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

                # Regola: Timestamp non valido
                try:
                    data['timestamp_ricevuto'] = datetime.strptime(data['timestamp_ricevuto'], '%Y-%m-%d %H:%M:%S')
                except (ValueError, TypeError):
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

        # Inserimento dei dati validi
        try:
            disable_foreign_keys(my_cursor)
            insert_query_valid = """
            INSERT INTO Forgiatura (codice_pezzo, peso_effettivo, temperatura_effettiva, timestamp, codice_macchinario)
            VALUES (%s, %s, %s, %s, %s)
            """
            if not save_records(my_cursor, my_conn, insert_query_valid, transformed_data):
                etl_status['last_error'] = 'Errore durante l\'inserimento dei dati validi in MySQL.'
            else:
                mysql_insert_success = True
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
            if not save_records(my_cursor, my_conn, insert_query_invalid, invalid_records):
                etl_status['last_error'] = 'Errore durante l\'inserimento dei dati non validi in MySQL.'
                mysql_insert_success = False
            else:
                mysql_insert_success = True
        except Exception as e:
            logging.error(f'Errore durante il salvataggio dei dati non validi: {e}')
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
            except psycog2.Error as e:
                logging.error(f'Errore durante l\'eliminazione dei dati in Postgres: {e}')
                etl_status['last_error'] = f'Errore durante l\'eliminazione in PostgreSQL: {e}'
                pg_conn.rollback()

    except Exception as e:
        logging.error(f'Errore generico: {e}')
        etl_status['last_error'] = str(e)

    finally:
        etl_status['running'] = False

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

if __name__ == '__main__':
    # Assicurati che la directory dei log esista
    if not os.path.exists('logs'):
        os.makedirs('logs')

    app.run(host='0.0.0.0', port=5000, debug=True)