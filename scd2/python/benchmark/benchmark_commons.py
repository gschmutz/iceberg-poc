import logging
import os
import re
import time
from datetime import date, datetime, timedelta
from typing import Optional

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
    
def replace_vars_in_string(s, variables):
    print(f"Replacing variables in string: {s} with {variables}")
    # Replace {var} with value from variables dict
    return re.sub(r"\{(\w+)\}", lambda m: str(variables.get(m.group(1), m.group(0))), s)

def fmt_checksum_cols(cols: list):
    return ", ".join(
        [f"checksum(CAST ({col} AS VARCHAR)) AS checksum_{col}" for col in cols]
    )

def _fqn(catalog: str, schema: str, table_name: str) -> str:
    """Return the fully-qualified Trino table name ``catalog.schema.table``."""
    return f"{catalog}.{schema}.{table_name}"

def optimize_table(conn, table_name: str) -> None:
    stmt = f"""
        ALTER TABLE {table_name}
        EXECUTE optimize (file_size_threshold => '256MB')
    """
    print(stmt)
    conn.cursor().execute(stmt)
    logger.info(f"Optimize table for {table_name} executed successfully.")

def _format_create_scd2_table(
    catalog_name: str,
    schema_name: str,
    table_name: str,
    s3_warehouse_bucket: str,
    s3_warehouse_prefix: str,
    pk_columns_with_type: list,
    cols_with_type: Optional[list],
    partitioning_cols: Optional[list],
    sort_cols: Optional[list],
) -> str:
    pk_str = ", ".join(pk_columns_with_type) if pk_columns_with_type else ""
    cols_str = ", ".join(cols_with_type) if cols_with_type else ""
    partitioning_str = ", ".join(f"'{c}'" for c in partitioning_cols) if partitioning_cols else ""
    sorted_by_str = ", ".join(f"'{c}'" for c in sort_cols) if sort_cols else ""

    return f"""
CREATE TABLE IF NOT EXISTS {_fqn(catalog_name, schema_name, table_name)} (
    dp_key VARCHAR,
    {pk_str},
    {cols_str},

    -- SCD2 metadata columns
    dp_ts_from TIMESTAMP,
    dp_ts_to TIMESTAMP,
    dp_is_active BOOLEAN,
    dp_is_latest BOOLEAN,
    dp_created_at TIMESTAMP,
    dp_replaced_at TIMESTAMP,

    -- Additional metadata
    dp_record_hash VARCHAR
)
WITH (
    partitioning = ARRAY[{partitioning_str}],
    sorted_by = ARRAY[{sorted_by_str}],
    location = 's3a://{s3_warehouse_bucket}/{s3_warehouse_prefix}/{schema_name}/{table_name}'
)
"""

def create_scd2_table(
    conn,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    s3_warehouse_bucket: str,
    s3_warehouse_prefix: str,
    pk_columns_with_type: list,
    cols_with_type: Optional[list] = None,
    partition_cols: Optional[list] = None,
    sort_cols: Optional[list] = None,
) -> None:
    drop_stmt = f"DROP TABLE IF EXISTS {_fqn(catalog_name, schema_name, table_name)}"
    print(drop_stmt)
    conn.cursor().execute(drop_stmt)

    create_stmt = _format_create_scd2_table(
        catalog_name=catalog_name,
        schema_name=schema_name,
        table_name=table_name,
        s3_warehouse_bucket=s3_warehouse_bucket,
        s3_warehouse_prefix=s3_warehouse_prefix,
        pk_columns_with_type=pk_columns_with_type,
        cols_with_type=cols_with_type,
        partitioning_cols=partition_cols,
        sort_cols=sort_cols,
    )
    print(create_stmt)
    conn.cursor().execute(create_stmt)
    logger.info(f"SCD2 table {table_name} created successfully.")

def merge_into_scd2_table_with_metrics(cursor, scd2, dp_ts) -> dict:
    start = time.perf_counter()
    success = True
    error = None

    try:
        scd2.merge_into_scd2_table(dp_ts=dp_ts)
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