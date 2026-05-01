import json
import logging
import os
import subprocess

import boto3

from config import DB_CONFIG, ENV
from parser import FlywayParser

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger("db-migrator.main")

TMP_DIR = "/tmp/migrations"

"""Main class for handling database migrations."""
class MigrationEngine:

    """Handles the database migration process."""
    def _prepare_artifacts(self, event):
        """Fetches migration scripts from local volume or S3."""
        # Use the ENV constant imported from config.py
        if ENV == "local":
            path = event.get("local_path", "./test")
            if not os.path.exists(path):
                raise FileNotFoundError(f"Local migration path not found: {path}")
            logger.info(f"ARTIFACTS: Using local path: {path}")
            return path

        # S3 Logic for AWS Environments
        bucket = event.get('bucket')
        key = event.get('key')

        if not bucket or not key:
            raise ValueError("CONFIG_ERROR: S3 bucket or key missing in event trigger")

        try:
            # Clean up previous runs in the Lambda ephemeral storage
            if os.path.exists(TMP_DIR):
                subprocess.run(["rm", "-rf", TMP_DIR], check=True)
            os.makedirs(TMP_DIR)

            zip_path = os.path.join("/tmp", "scripts.zip")
            logger.info(f"S3: Downloading s3://{bucket}/{key} to {zip_path}")

            s3 = boto3.client('s3')
            s3.download_file(bucket, key, zip_path)

            # Unzip scripts to the TMP_DIR
            result = subprocess.run(
                ["unzip", "-o", zip_path, "-d", TMP_DIR],
                capture_output=True,
                text=True
            )

            if result.returncode != 0:
                raise Exception(f"UNZIP_ERROR: {result.stderr}")

            logger.info(f"ARTIFACTS: Successfully extracted to {TMP_DIR}")
            return TMP_DIR

        except Exception as e:
            # Re-wrap as an Artifact Error for the main 'run' method to catch
            raise Exception(f"ARTIFACT_ERROR: {str(e)}")

    def _execute_flyway(self, source, command):
        jdbc = f"jdbc:mysql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

        extra_args = []
        if command == "validate":
            extra_args = ["-ignoreMigrationPatterns=*:pending"]
        elif command == "info":
            extra_args = ["-outputType=json"]

        cmd = [
            "flyway",
            f"-url={jdbc}",
            f"-user={DB_CONFIG['user']}",
            f"-password={DB_CONFIG['password']}",
            f"-locations=filesystem:{source}",
            "-validateMigrationNaming=true",
            command
        ]
        if extra_args: cmd.extend(extra_args)

        logger.info(f"Flyway: Connecting to [{DB_CONFIG['host']}], running command: [{command}] with args: {extra_args}")

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        output = []
        for line in process.stdout:
            print(line, end='')
            output.append(line)
        process.wait()

        return process.returncode, "".join(output)

    def run(self, event):
        try:
            logger.info("STARTING_MIGRATION")

            command = event.get("command", "migrate").lower()
            is_safe, error_msg = FlywayParser.is_command_safe(command)
            if not is_safe:
                return {
                    "success": False,
                    "status": "FORBIDDEN_COMMAND",
                    "message": error_msg
                }

            source = "./test" if ENV == "local" else self._prepare_artifacts(event)

            # Pre-flight Validation
            if command == "migrate":
                code, out = self._execute_flyway(source, "validate")

                target_table = "flyway_schema_history"
                history_msg = f"Schema history table `{DB_CONFIG['database']}`.`{target_table}` does not exist yet"

                if code != 0 and history_msg not in out:
                    return FlywayParser.parse(code, out, "validate")

                logger.info("PRE-FLIGHT_VALIDATION_PASSED")

            # Main Execution
            code, out = self._execute_flyway(source, command)

            response = FlywayParser.parse(code, out, command)

            return response

        except Exception as e:
            logger.error(f"FATAL: {str(e)}")
            return {"success": False, "status": "SETUP_ERROR", "message": str(e)}

def lambda_handler(event, context):
    return MigrationEngine().run(event)

# --- Local Testing ---
if __name__ == "__main__":
    test_event = {
        "local_path": "./test",
        "command": "migrate"
    }

    print(json.dumps(lambda_handler(test_event, None), indent=2))