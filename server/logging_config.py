import logging
import os

def configure_logging():
    """
    Configura i logger per l'applicazione:
    - logger principale (etl.log)
    - logger periodico (periodic.log)
    """
    # Impostazione del logger principale
    logging.basicConfig(
        filename='logs/etl.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Logger per attività periodiche
    periodic_logger = logging.getLogger('periodic_logger')
    periodic_logger.setLevel(logging.INFO)

    # Handler per il file di log periodico
    periodic_handler = logging.FileHandler('logs/periodic.log')
    periodic_handler.setLevel(logging.INFO)
    periodic_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    periodic_handler.setFormatter(periodic_formatter)

    # Aggiunge l'handler al logger periodico
    periodic_logger.addHandler(periodic_handler)

    return periodic_logger