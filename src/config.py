import os
import json
import boto3
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger("db-migrator.config")

ENV = os.getenv("ENV", "local")

""" Setup the database configuration """
def get_db_config():
    if ENV == "local":
        return {
            "host": os.getenv("DB_HOST"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "database": os.getenv("DB_NAME"),
            "port": int(os.getenv("DB_PORT", 3306)),
        }

    try:
        ssm = boto3.client('ssm')
        param_path = f"/{ENV}/phase-1/db_config"
        response = ssm.get_parameter(Name=param_path, WithDecryption=True)
        return json.loads(response['Parameter']['Value'])
    except Exception as e:
        logger.critical(f"BOOT_FAILURE: {str(e)}")
        return {"BOOT_ERROR": str(e)}

DB_CONFIG = get_db_config()