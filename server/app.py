import os
import psycopg2
import mysql.connector
import logging
from dotenv import load_dotenv
from datetime import datetime
from flask import Flask, jsonify, request
from threading import Thread

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
        pg_cursor.execute("SELECT * FROM RawForgiatura WHERE stato_processo = 'PENDING'")
        raw_data = pg_cursor.fetchall()
        colnames = [desc[0] for desc in pg_cursor.description]
        logging.info(f'Estrazione di {len(raw_data)} record da PostgreSQL.')

        # Trasformazione dei dati
        transformed_data = []
        for row in raw_data:
            data = dict(zip(colnames, row))
            try:
                if isinstance(data['timestamp_ricevuto'], str):
                    data['timestamp_ricevuto'] = datetime.strptime(data['timestamp_ricevuto'], '%Y-%m-%d %H:%M:%S')

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

        data_processed['data'] = transformed_data

        # Caricamento dei dati in MySQL
        try:
            disable_foreign_keys(my_cursor)

            insert_query = """
            INSERT INTO Forgiatura (codice_pezzo, peso_effettivo, temperatura_effettiva, timestamp, codice_macchinario)
            VALUES (%s, %s, %s, %s, %s)
            """
            my_cursor.executemany(insert_query, transformed_data)
            my_conn.commit()
            logging.info(f'Inseriti {my_cursor.rowcount} record in MySQL.')

            enable_foreign_keys(my_cursor)

            logging.info(f"etl_status type: {type(etl_status)}, value: {etl_status}")

            # Preparazione degli ID processati
            processed_ids = [data[0] for data in raw_data if isinstance(data[0], int)]
            logging.info(f"ID MySQL Processati: {processed_ids}")

            if not processed_ids:
                logging.warning('Nessun ID valido trovato in raw_data. Nessun aggiornamento eseguito in PostgreSQL.')
            else:
                update_query = """
                UPDATE RawForgiatura
                SET stato_processo = %s
                WHERE id = ANY(%s)
                """
                pg_cursor.execute(update_query, ('PROCESSED', processed_ids))
                pg_conn.commit()
                logging.info(f'Aggiornato lo stato di {pg_cursor.rowcount} record in PostgreSQL.')

            etl_status['last_success'] = datetime.utcnow().isoformat()
            etl_status['last_error'] = None

        except mysql.connector.Error as e:
            logging.error(f'Errore durante l\'inserimento dei dati in MySQL: {e}')
            if my_conn.is_connected():
                my_conn.rollback()
            etl_status['last_error'] = str(e)

        except psycopg2.Error as e:
            logging.error(f'Errore durante l\'aggiornamento dei dati in PostgreSQL: {e}')
            if pg_conn:
                pg_conn.rollback()
            etl_status['last_error'] = str(e)

        except Exception as e:
            logging.error(f'Errore generico: {e}')
            etl_status['last_error'] = str(e)

        finally:
            # Chiusura delle connessioni MySQL
            if 'my_cursor' in locals() and my_cursor:
                my_cursor.close()
            if 'my_conn' in locals() and my_conn.is_connected():
                my_conn.close()
                logging.info('Connessione a MySQL chiusa.')

            # Chiusura delle connessioni PostgreSQL
            if 'pg_cursor' in locals() and pg_cursor:
                pg_cursor.close()
            if 'pg_conn' in locals() and pg_conn:
                pg_conn.close()
                logging.info('Connessione a PostgreSQL chiusa.')

    except Exception as e:
        logging.error(f'Connessione ai database fallita: {e}')
        etl_status['last_error'] = str(e)

    etl_status['running'] = False

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