import logging
import os
import re
import time

from sqlalchemy import create_engine, text
from datetime import date, timedelta, datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# will only be used when running inside a scenario in Dataiku
try:
    from dataiku.scenario import Scenario
    # This will only succeed if running inside DSS
    scenario = Scenario()
except ImportError:
    logger.info("Unable to setup dataiku scenario API due to import error")    
    scenario = None
 
# will only be used when running inside a scenario in Dataiku
try:
    import dataiku
    # This will only succeed if running inside DSS
    client = dataiku.api_client()
except ImportError:
    logger.info("Unable to setup dataiku client API due to import error")
    client = None
 
def get_param(name, default=None, upper=False) -> str:
    """
    Retrieves the value of a parameter by name from the scenario variables if available,
    otherwise from the environment variables.
 
    Args:
        name (str): The name of the parameter to retrieve.
        default (Any, optional): The default value to return if the parameter is not found. Defaults to None.
 
    Returns:
        Any: The value of the parameter if found, otherwise the default value.
    """
    return_value = default
    if scenario is not None:
        return_value = scenario.get_all_variables().get(name, default)
    else:
        return_value = os.getenv(name, default)

    logger.info(f"{name}: {return_value}")

    if upper:
        return return_value.upper()
    else:
        return return_value

def get_credential(name, default=None) -> str:
    """
    Retrieves the value of a secret credential by its name.
    Args:
        name (str): The key name of the credential to retrieve.
        default (str, optional): The default value to return if the credential is not found. Defaults to None.
    Returns:
        str: The value of the credential if found, otherwise the default value.
    """
    return_value = default
    if client is not None:
        secrets = client.get_auth_info(with_secrets=True)["secrets"]
        for secret in secrets:
            if secret["key"] == name:
                if "value" in secret:
                    return_value = secret["value"]
                else:
                    break
    else:
        return_value = os.getenv(name, default)
    logger.info(f"{name}: *****")
         
    return return_value

def get_zone_name(upper=False) -> str:
    """
    Retrieves the zone name from the Dataiku global variable.
    """
    return_value = "unknown"
    if scenario is not None:
        env_var = scenario.get_all_variables().get("env")
        if env_var == "des":
            return_value = "sz"
        elif env_var == "des_pz":
            return_value = "pz"
    else:
        return_value = os.getenv("DATAIKU_ENV", "unknown")

    logger.info(f"Zone: {return_value}")

    if upper:
        return return_value.upper()
    else:
        return return_value

def get_run_id() -> str:
    """
    Retrieves the run ID from the Dataiku scenario if available, otherwise generates a new UUID.
    Returns:
        str: The run ID.
    """
    return_value = None
    if scenario is not None:
        return_value = scenario.get_all_variables().get("scenarioTriggerRunId")
    if not return_value:
        import uuid
        return_value = str(uuid.uuid4())
    logger.info(f"Run ID: {return_value}")
    return return_value

def get_run_url() -> str:
    """
    Retrieves the run URL from the Dataiku scenario if available, otherwise returns an empty string.
    Returns:
        str: The run URL.
    """
    return_value = None
    if scenario is not None:
        return_value = scenario.get_all_variables().get("scenarioRunURL")
    if not return_value:
        return_value = ""
    logger.info(f"Run URL: {return_value}")
    return return_value

def replace_vars_in_string(s, variables):
    print(f"Replacing variables in string: {s} with {variables}")
    # Replace {var} with value from variables dict
    return re.sub(r"\{(\w+)\}", lambda m: str(variables.get(m.group(1), m.group(0))), s)        

def execute_with_metrics(cursor, sql: str) -> dict:
    start = time.perf_counter()
    success = True
    error = None

    try:
        cursor.execute(sql)
        cursor.fetchall()
    except Exception as e:
        success = False
        error = str(e)

    elapsed_ms = int((time.perf_counter() - start) * 1000)
    stats = cursor.stats or {}

    return {
        "query_id": stats.get("queryId"),
        "elapsed_ms": elapsed_ms,
        "cpu_ms": stats.get("cpuTimeMillis"),
        "queued_ms": stats.get("queuedTimeMillis"),
        "processed_rows": stats.get("processedRows"),
        "processed_bytes": stats.get("processedBytes"),
        "success": success,
        "error": error,
        "executed_at": datetime.utcnow(),
    }        