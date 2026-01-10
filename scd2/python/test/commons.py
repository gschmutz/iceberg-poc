DIM_TABLE_NAME="dim_person"
RAW_TABLE_NAME="raw_person"
SCD2_VIEW_NAME="view_person_scd2"

EXCLUDE_COLS = ["record_hash","dp_load_timestamp", "change_type"]
LOAD_TS_COL="dp_exported_at"
