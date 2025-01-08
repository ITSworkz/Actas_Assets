import pandas as pd
import pdfkit
from jinja2 import Environment, FileSystemLoader
from sqlalchemy import create_engine
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os
import logging
from tqdm import tqdm
from dotenv import load_dotenv
from datetime import datetime
import sys

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Configuración para Google Drive
CREDENTIALS_FILE = os.getenv('CREDENTIALS_FILE')
SHARED_DRIVE_ID = os.getenv('SHARED_DRIVE_ID')  # ID real del Shared Drive
FOLDER_ID = os.getenv('FOLDER_ID')  # ID de la carpeta dentro del Shared Drive

# Configurar el logger
logs_dir = 'logs'
os.makedirs(logs_dir, exist_ok=True)
log_file = os.path.join(logs_dir, 'Acta_Assets.log')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(log_file),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

def generar_reporte(usuario):
    try:
        # Crear la conexión a la base de datos usando SQLAlchemy
        engine = create_engine('mysql+pymysql://root:Sworkz.80@localhost/sys')

        # Consulta SQL con los nuevos campos: tipo_item, manufacturer, serial y uuid
        query = """
        SELECT 
            u.name AS usuario,
            'Computadora' AS tipo_item,
            c.name AS item,
            c.serial,
            m.name AS manufacturer,
            c.uuid AS uuid,
            c.id AS item_id
        FROM 
            glpi_users u
        JOIN 
            glpi_computers c ON c.users_id = u.id
        LEFT JOIN 
            glpi_manufacturers m ON c.manufacturers_id = m.id
        WHERE 
            u.name = %s

        UNION ALL

        SELECT 
            u.name AS usuario,
            'Monitor' AS tipo_item,
            m2.name AS item,
            m2.serial,
            manuf.name AS manufacturer,
            m2.uuid AS uuid,
            m2.id AS item_id
        FROM 
            glpi_users u
        JOIN 
            glpi_monitors m2 ON m2.users_id = u.id
        LEFT JOIN 
            glpi_manufacturers manuf ON m2.manufacturers_id = manuf.id
        WHERE 
            u.name = %s

        UNION ALL

        SELECT 
            u.name AS usuario,
            p.name AS tipo_item,
            p.name AS item,
            p.serial,
            manuf2.name AS manufacturer,
            p.uuid AS uuid,
            p.id AS item_id
        FROM 
            glpi_users u
        JOIN 
            glpi_peripherals p ON p.users_id = u.id
        LEFT JOIN 
            glpi_manufacturers manuf2 ON p.manufacturers_id = manuf2.id
        WHERE 
            u.name = %s;
        """

        # Leer los datos desde la base de datos de manera segura
        df = pd.read_sql(query, con=engine, params=(usuario, usuario, usuario))

        # Verificar si se cargaron datos
        if df.empty:
            print(f"No se encontraron activos asociados al usuario: {usuario}")
            return

        df['price'] = "$0"
        # Cargar la plantilla con jinja2
        env = Environment(loader=FileSystemLoader('.'))
        template = env.get_template('plantilla.html')

        # Renderizar la plantilla con los datos
        html_out = template.render(items=df.to_dict(orient='records'))

        # Configuración de PDF
        config = pdfkit.configuration(wkhtmltopdf='/usr/bin/wkhtmltopdf')

        # Obtener la fecha actual
        fecha_actual = datetime.now().strftime("%d-%m-%Y")

        # Nombre del archivo PDF
        pdf_file = f'{usuario}_Assets_{fecha_actual}.pdf'

        # Generar el PDF
        options = {
            'quiet': '',
            'no-stop-slow-scripts': '',
            'enable-local-file-access': ''
        }
        pdfkit.from_string(html_out, pdf_file, configuration=config, options=options)

        print(f"PDF generado exitosamente para el usuario {usuario}.")

        # Subir el archivo PDF a Google Drive
        upload_file(pdf_file, CREDENTIALS_FILE)

        # Eliminar el archivo PDF localmente después de subirlo
        os.remove(pdf_file)
        print(f"El archivo {pdf_file} fue eliminado localmente después de ser subido.")

    except Exception as e:
        print(f"Error al generar el PDF: {e}")

def upload_file(file_name, CREDENTIALS_FILE):
    """Sube el archivo PDF a Google Drive."""
    try:
        # Autenticación con la cuenta de servicio para Google Drive
        credentials_drive = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=['https://www.googleapis.com/auth/drive'])

        # Construir el servicio de Google Drive
        service_drive = build('drive', 'v3', credentials=credentials_drive)

        # Metadatos del archivo a subir
        file_metadata = {
            'name': os.path.basename(file_name),
            'parents': [FOLDER_ID],  # Carpeta en el Shared Drive
            'driveId': SHARED_DRIVE_ID
        }

        # Preparar el archivo para subir
        media = MediaFileUpload(file_name, mimetype='application/pdf', resumable=True)
        request = service_drive.files().create(
            body=file_metadata, media_body=media, fields='id', supportsAllDrives=True)

        # Ejecutar la subida y mostrar progreso
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                logger.info(f"Subiendo {file_name}... {int(status.progress() * 100)}% completado")
                tqdm(total=100, initial=status.progress() * 100)

        logger.info(f"Archivo subido con ID: {response.get('id')}")

    except Exception as e:
        logger.error(f"Error al subir el archivo a Google Drive: {e}")

if __name__ == '__main__':
    # Verificar que se pasó el nombre de usuario como argumento
    if len(sys.argv) < 2:
        print("Uso: python script.py <usuario>")
        sys.exit(1)

    # Obtener el nombre del usuario desde los argumentos
    usuario = sys.argv[1]

    # Generar el reporte para el usuario especificado
    generar_reporte(usuario)
