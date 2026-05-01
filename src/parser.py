import json
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger("db-migrator.parser")

class FlywayParser:
    # Strict Allowlist: If it's not here, it doesn't run.
    ALLOWED_COMMANDS = ["migrate", "info", "repair", "validate"]

    @staticmethod
    def is_command_safe(cmd):
        """Single gatekeeper for all Flyway actions."""
        if cmd in FlywayParser.ALLOWED_COMMANDS:
            return True, "OK"

        return False, f"Forbidden: Command '{cmd}' is not in the allowed list ({', '.join(FlywayParser.ALLOWED_COMMANDS)})."

    @staticmethod
    def parse(exit_code, output, command):
        """ Parses the output of the Flyway migration engine. """
        if exit_code != 0:
            return FlywayParser._handle_error(output, command)
        return FlywayParser._handle_success(output, command)

    @staticmethod
    def _handle_error(output, command):
        lines = [l.strip() for l in output.splitlines()]
        out_low = output.lower()

        # Get the structured headers Flyway provides
        sql_state = next((l.split(":")[-1].strip() for l in lines if l.startswith("SQL State")), "")
        db_msg = next((l.split(":", 1)[-1].strip() for l in lines if l.startswith("Message")), "")
        location = next((l.split(":", 1)[-1].strip() for l in lines if l.startswith("Location")), "")
        line_num = next((l.split(":", 1)[-1].strip() for l in lines if l.startswith("Line")), "Unknown")

        # Map SQL State to Error Types
        # 08XXX = Connection issues | 28XXX = Auth issues | 42XXX = Syntax/Config issues
        if sql_state.startswith("08"):
            status, category = "INFRA_ERROR", "DATABASE_UNREACHABLE"

        elif sql_state.startswith("28"):
            status, category = "AUTH_ERROR", "ACCESS_DENIED"

        elif sql_state == "42000" and "Unknown database" in db_msg:
            status, category = "CONFIG_ERROR", "UNKNOWN_DATABASE"

        elif "invalid sql filenames" in out_low:
            status, category = "DEVELOPER_ERROR", "NAMING_CONVENTION_ERROR"

        elif "failed to execute script" in out_low:
            status, category = "DEVELOPER_ERROR", "SQL_SYNTAX_ERROR"

        elif any(x in out_low for x in ["validate failed", "failed migration"]):
            status, category = "VALIDATION_ERROR", "DATABASE_TAINTED"

        else:
            status, category = "EXECUTION_ERROR", "UNKNOWN_FLYWAY_ERROR"

        # Build the final message
        if category == "NAMING_CONVENTION_ERROR":
            bad_files = [l.split("format:")[-1].strip().split(" ")[0] for l in lines if "Invalid versioned migration" in l]
            msg = f"{category}: [{', '.join(bad_files)}]. Use format 'V1__desc.sql'."

        elif category == "SQL_SYNTAX_ERROR":
            file = location.split("/")[-1].replace(")", "") or "Unknown Script"
            msg = (
                f"MIGRATION_FAILED: {file} at Line {line_num}. Details: {db_msg}. "
                "ACTION REQUIRED: 1. Fix the issue in the script (ensure it is idempotent). "
                "2. Trigger 'repair' command manually. "
                "3. Commit changes and re-trigger migration."
            )

        else:
            msg = f"{category}: {db_msg or 'Check Flyway logs.'}"

        return {"success": False, "status": status, "message": msg}

    @staticmethod
    def _handle_success(output, command):
        """ Helper method - handles parsing of successful Flyway executions."""
        lines = [l.strip() for l in output.splitlines() if l.strip()]
        resp = {"success": True, "command": command}

        if command == "info":
            return FlywayParser._parse_info_json(output, resp)

        if command == "migrate":
            prev = next((l.split(":")[-1].strip() for l in lines if "Current version of schema" in l), "0")
            curr = next((l.split("version")[-1].split("(")[0].strip() for l in lines if "now at version v" in l), prev)
            resp.update({"status": "UPGRADED" if "Successfully applied" in output else "NO_CHANGE", "previous_v": prev, "current_v": curr})

        elif command == "repair":
            summary = [l for l in lines if "repaired" in l.lower() or "not necessary" in l.lower()]
            resp["message"] = " ".join(summary)

        elif command == "validate":
            resp["status"] = "VALIDATION_SUCCESS"
            resp["message"] = "All migrations are valid."

        return resp

    @staticmethod
    def _parse_info_json(output, resp):
        """ Helper method - parses the JSON output of 'info' command. """
        try:
            json_line = next((l for l in output.splitlines() if l.strip().startswith('{')), None)
            if not json_line:
                resp.update({"status": "INFO_PARSE_FAILED", "message": "No JSON found in Flyway output"})
                return resp
            data = json.loads(str(json_line))
            table = [{"v": m.get("version"), "status": m.get("state"), "date": m.get("installedOnUTC")}
                     for m in data.get("migrations", [])]
            resp.update({
                "status": "INFO_SUCCESS",
                "details": {"engine": data.get('flywayVersion'), "current_v": data.get('schemaVersion', "0"), "table": table}
            })
        except Exception as e:
            resp.update({"status": "INFO_PARSE_FAILED", "message": str(e)})
        return resp