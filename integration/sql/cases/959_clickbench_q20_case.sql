-- description: ClickBench Citus query 20
-- tags: sharded
-- transactional: true
-- only-targets: postgres_standard_text pgdog_sharded_text pgdog_sharded_binary

SELECT UserID FROM hits WHERE UserID = 435090932899640449;
