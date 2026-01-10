USE iceberg_hive.default;

CREATE OR REPLACE VIEW benchmark_by_test_case_v 
AS
SELECT b.statement_key,b.statement_name, b.case_id, b.tshirt_size, b.day_number
	, array_agg( format ('%.2f', round(cast (elapsed_ms as double) / 1000, 2))) as elapsed_s 
	, min(iceberg_nof_files) as iceberg_nof_files
FROM benchmark b
WHERE success = True
GROUP BY b.statement_key,b.statement_name, b.case_id, b.tshirt_size, b.day_number
ORDER BY statement_key,b.day_number ASC;

CREATE OR REPLACE VIEW benchmark_scd2_merge_report_v 
AS
SELECT
    day_number,
    max(CASE WHEN statement_key = 'SCD2_MERGE_1_l' THEN elapsed_s END) AS es_ts_1_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_2_l' THEN elapsed_s END) AS es_ts_2_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_3_l' THEN elapsed_s END) AS es_ts_3_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_4_l' THEN elapsed_s END) AS es_ts_4_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_5_l' THEN elapsed_s END) AS es_ts_5_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_6_l' THEN elapsed_s END) AS es_ts_6_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_7_l' THEN elapsed_s END) AS es_ts_7_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_8_l' THEN elapsed_s END) AS es_ts_8_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_9_l' THEN elapsed_s END) AS es_ts_9_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_10_l' THEN elapsed_s END) AS es_ts_10_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_11_l' THEN elapsed_s END) AS es_ts_11_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_12_l' THEN elapsed_s END) AS es_ts_12_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_13_l' THEN elapsed_s END) AS es_ts_13_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_14_l' THEN elapsed_s END) AS es_ts_14_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_15_l' THEN elapsed_s END) AS es_ts_15_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_1_l' THEN iceberg_nof_files END) AS nof_files_1_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_2_l' THEN iceberg_nof_files END) AS nof_files_2_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_3_l' THEN iceberg_nof_files END) AS nof_files_3_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_4_l' THEN iceberg_nof_files END) AS nof_files_4_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_5_l' THEN iceberg_nof_files END) AS nof_files_5_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_6_l' THEN iceberg_nof_files END) AS nof_files_6_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_7_l' THEN iceberg_nof_files END) AS nof_files_7_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_8_l' THEN iceberg_nof_files END) AS nof_files_8_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_9_l' THEN iceberg_nof_files END) AS nof_files_9_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_10_l' THEN iceberg_nof_files END) AS nof_files_10_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_11_l' THEN iceberg_nof_files END) AS nof_files_11_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_12_l' THEN iceberg_nof_files END) AS nof_files_12_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_13_l' THEN iceberg_nof_files END) AS nof_files_13_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_14_l' THEN iceberg_nof_files END) AS nof_files_14_l,
    max(CASE WHEN statement_key = 'SCD2_MERGE_15_l' THEN iceberg_nof_files END) AS nof_files_15_l
FROM benchmark_by_test_case_v
WHERE statement_key like 'SCD2_MERGE%' AND tshirt_size = 'l'
GROUP by day_number
ORDER BY
    day_number;

CREATE OR REPLACE VIEW benchmark_scd2_query_report_v 
AS
SELECT
    day_number,
    regexp_replace(statement_key, '_[0-9]+_[a-zA-Z]$','') as base_statement_key,
    max(CASE WHEN case_id = '1' THEN elapsed_s END) AS es_ts_1,
    max(CASE WHEN case_id = '2' THEN elapsed_s END) AS es_ts_2,
    max(CASE WHEN case_id = '3' THEN elapsed_s END) AS es_ts_3,
    max(CASE WHEN case_id = '4' THEN elapsed_s END) AS es_ts_4,
    max(CASE WHEN case_id = '5' THEN elapsed_s END) AS es_ts_5,
    max(CASE WHEN case_id = '6' THEN elapsed_s END) AS es_ts_6,
    max(CASE WHEN case_id = '7' THEN elapsed_s END) AS es_ts_7,
    max(CASE WHEN case_id = '8' THEN elapsed_s END) AS es_ts_8,
    max(CASE WHEN case_id = '9' THEN elapsed_s END) AS es_ts_9,
    max(CASE WHEN case_id = '10' THEN elapsed_s END) AS es_ts_10,
    max(CASE WHEN case_id = '11' THEN elapsed_s END) AS es_ts_11,
    max(CASE WHEN case_id = '12' THEN elapsed_s END) AS es_ts_12,
    max(CASE WHEN case_id = '13' THEN elapsed_s END) AS es_ts_13,
    max(CASE WHEN case_id = '14' THEN elapsed_s END) AS es_ts_14,
    max(CASE WHEN case_id = '15' THEN elapsed_s END) AS es_ts_15
FROM benchmark_by_test_case_v
WHERE statement_key like 'SCD2_SELECT%' AND tshirt_size = 'l'
GROUP by day_number, regexp_replace(statement_key, '_[0-9]+_[a-zA-Z]$','')
ORDER BY
    day_number;  