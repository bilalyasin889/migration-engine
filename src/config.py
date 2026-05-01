import os
import json
import boto3
import logging
from typing import Tuple

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger("db-migrator.config")

ENV = os.getenv("ENV", "local")

REQUIRED_KEYS = {"host", "user", "password", "database", "port"}

def get_db_config():
    global ssm
    if ENV == "local":
        return {
            "host": os.getenv("DB_HOST"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "database": os.getenv("DB_NAME"),
            "port": int(os.getenv("DB_PORT", 3306)),
        }

    param_path = f"/{ENV}/phase-1/db_config"
    try:
        ssm = boto3.client('ssm')
        response = ssm.get_parameter(Name=param_path, WithDecryption=True)
        return json.loads(response['Parameter']['Value'])

    except ssm.exceptions.ParameterNotFound:
        logger.critical(f"BOOT_FAILURE: SSM parameter not found: {param_path}")
        return {"BOOT_ERROR": "SSM parameter not found"}

    except ssm.exceptions.AccessDeniedException:
        logger.critical(f"BOOT_FAILURE: Access denied to SSM parameter: {param_path}")
        return {"BOOT_ERROR": "Access denied to SSM parameter"}

    except json.JSONDecodeError as e:
        logger.critical(f"BOOT_FAILURE: SSM parameter is not valid JSON: {str(e)}")
        return {"BOOT_ERROR": f"SSM parameter is not valid JSON: {str(e)}"}

    except Exception as e:
        logger.critical(f"BOOT_FAILURE: {str(e)}")
        return {"BOOT_ERROR": str(e)}

def validate_db_config(config: dict) -> Tuple[bool, str]:
    if "BOOT_ERROR" in config:
        logger.critical(f"BOOT_FAILURE: {config['BOOT_ERROR']}")
        return False, f"Database config failed to load: {config['BOOT_ERROR']}"

    missing_keys = REQUIRED_KEYS - config.keys()
    if missing_keys:
        logger.critical(f"BOOT_FAILURE: Missing keys in config: {missing_keys}")
        return False, f"Database config is missing required keys: {', '.join(missing_keys)}"

    empty_values = {k for k in REQUIRED_KEYS if not config.get(k)}
    if empty_values:
        logger.critical(f"BOOT_FAILURE: Empty values in config for keys: {empty_values}")
        return False, f"Database config has empty values for keys: {', '.join(empty_values)}"

    return True, "OK"

DB_CONFIG = get_db_config()