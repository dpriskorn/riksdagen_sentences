SELECT table_schema AS "Database", 
       ROUND(SUM(data_length + index_length) / (1024 * 1024)) AS "Size (MB)" 
FROM information_schema.TABLES 
WHERE table_schema = 'riksdagen'
GROUP BY table_schema;
