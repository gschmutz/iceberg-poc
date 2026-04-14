def fmt_checksum_cols(cols: list):
    return ", ".join(
        [f"checksum(CAST ({col} AS VARCHAR)) AS checksum_{col}" for col in cols]
    )
