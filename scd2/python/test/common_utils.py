import logging
import os
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def replace_vars_in_string(s, variables):
    print(f"Replacing variables in string: {s} with {variables}")
    # Replace {var} with value from variables dict
    return re.sub(r"\{(\w+)\}", lambda m: str(variables.get(m.group(1), m.group(0))), s)

def dict_to_tuple_in_rows(rows):
    return [
        tuple(tuple(v.values()) if isinstance(v, dict) else v for v in row)
        for row in rows
    ]