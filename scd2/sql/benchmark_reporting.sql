CREATE OR REPLACE VIEW iceberg_hive.default.benchmark_by_test_case_v 
AS
SELECT b.strategy,b.statement_name, b.case_id, b.tshirt_size, b.day_number
	, array_agg( format ('%.2f', round(cast (elapsed_ms as double) / 1000, 2))) as elapsed_s 
	, min(iceberg_nof_files) as iceberg_nof_files
FROM iceberg_hive.default.benchmark b
WHERE success = True
group by b.strategy,b.statement_name, b.case_id, b.tshirt_size, b.day_number
order by strategy,b.day_number asc;