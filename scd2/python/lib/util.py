import logging
import time
from datetime import date, datetime, timedelta

import pandas as pd
from tabulate import tabulate

class CustomLogAdapter(logging.LoggerAdapter):
    # Add ! around message, so it is easier to search in dataiku log
    def process(self, msg: str, kwargs):
        return f"{'!'* 10} {msg} {'!'* 10}", kwargs
 
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = CustomLogAdapter(logging.getLogger(__name__), {})

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


VOLATILE_IDX = {9, 10, 11}


def strip_volatile(row, volatile_idx: set):
    return tuple(v for i, v in enumerate(row) if i not in volatile_idx)


def normalize_row(row):
    return tuple(
        v.strftime("%Y-%m-%d %H:%M:%S") if isinstance(v, datetime) else v for v in row
    )


def get_table_data(
    conn,
    fully_qualified_table_name: str,
    exclude_cols: list = [],
    order_by_cols: list = [],
    for_version: str = None,
    output_file=None,
):
    if conn is not None:
        cursor = conn.cursor()
    # Build the column list, excluding columns in exclude_cols
    if exclude_cols:
        cursor.execute(f"SELECT * FROM {fully_qualified_table_name} LIMIT 0")
        columns = [desc[0] for desc in cursor.description]
        selected_columns = [col for col in columns if col not in exclude_cols]
        column_list = ", ".join(selected_columns)
    else:
        column_list = "*"

    # Build ORDER BY clause only if order_by_cols is not empty
    order_by_clause = (
        f"ORDER BY {', '.join(f'{col} NULLS LAST' for col in order_by_cols)}"
        if order_by_cols
        else ""
    )

    if for_version is not None:
        fully_qualified_table_name = (
            f"{fully_qualified_table_name} FOR VERSION AS OF {for_version}"
        )

    sql = f"""
        SELECT {column_list}
        FROM {fully_qualified_table_name}
        {order_by_clause}
        """
    df = pd.read_sql_query(sql, conn)
    return df


def diff_with_color(df1, df2, index_cols=None, sort_cols=None):
    """
    df1 = old / expected
    df2 = new / actual
    index_cols = list of columns that form the row key
    """

    index_cols = index_cols or []

    # --- Preserve final column order ---
    final_cols = list(df1.columns)
    for col in df2.columns:
        if col not in final_cols:
            final_cols.append(col)

    df1 = df1.reindex(columns=final_cols)
    df2 = df2.reindex(columns=final_cols)

    # --- Set index for diff logic ---
    if index_cols:
        df1_idx = df1.set_index(index_cols)
        df2_idx = df2.set_index(index_cols)
    else:
        df1_idx = df1
        df2_idx = df2

    merged = df1_idx.merge(
        df2_idx,
        how="outer",
        left_index=True,
        right_index=True,
        suffixes=("_old", "_new"),
        indicator=True,
    )
    # Add this line:
    sort_cols = [f"{col}_new" for col in sort_cols] if sort_cols else sort_cols
    merged = merged.sort_values(sort_cols) if sort_cols else merged

    rows = []

    for idx, row in merged.iterrows():
        output_row = []

        for col in final_cols:
            if col in index_cols:
                # index column value
                if isinstance(idx, tuple):
                    val = idx[index_cols.index(col)]
                else:
                    val = idx

                # Color index column if row is inserted or deleted
                if row["_merge"] == "right_only":
                    cell = f"<span style='color: green;'>{val}</span>"
                elif row["_merge"] == "left_only":
                    cell = f"<span style='color:gray;'>{val}</span>"
                else:
                    cell = str(val)

            else:
                old = row.get(f"{col}_old")
                new = row.get(f"{col}_new")

                if row["_merge"] == "left_only":
                    cell = f"<span style='color:gray;'>{old}</span>"
                elif row["_merge"] == "right_only":
                    cell = f"<span style='color: green;'>{new}</span>"
                else:  # both
                    try:
                        differs = bool(old != new)
                    except (ValueError, TypeError):
                        differs = old != new  # e.g. list: direct inequality is fine
                        if not isinstance(differs, bool):
                            differs = list(old) != list(new)
                    if differs:
                        cell = f"<span style='color: orange;'>{new}</span>"
                    else:
                        cell = str(new)

            output_row.append(cell)

        rows.append(output_row)

    result_df = pd.DataFrame(rows, columns=final_cols)
    return result_df


def render_init(title: str, output_file_name: str):
    if output_file_name:
        with open(output_file_name, "w") as f:
            f.write(f"# {title}\n\n")


def render_data(data: str, output_file_name=None):
    if data and output_file_name:
        with open(output_file_name, "a") as f:
            f.write(data + "\n")


def render_table(
    df,
    title: str = "",
    decscription: str = "",
    include_cols: list = [],
    exclude_cols: list = [],
    output_file_name=None,
):
    # Build the column list, including only columns in include_cols
    if include_cols:
        selected_columns = [col for col in include_cols if col in df.columns]
        df = df[selected_columns]

    # Build the column list, excluding columns in exclude_cols
    if exclude_cols:
        selected_columns = [col for col in df.columns if col not in exclude_cols]
        df = df[selected_columns]

    table_output = ""
    if title:
        table_output += f"\n\n**{title}**\n\n"
    if decscription:
        table_output += f"{decscription}\n\n"
    table_output += "\n"
    table_output += tabulate(df, headers=df.columns, tablefmt="github", showindex=False)
    table_output += "\n"
    if exclude_cols:
        table_output += (
            "\n_the following columns where excluded from the result: `"
            + ", ".join(exclude_cols)
            + "`_\n"
        )

    if output_file_name:
        with open(output_file_name, "a") as f:
            f.write(table_output + "\n")

    print(tabulate(df, headers=df.columns, tablefmt="github", showindex=False))
