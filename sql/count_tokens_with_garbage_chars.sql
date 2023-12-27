SELECT COUNT(*) as garbage_tokens
FROM rawtoken
WHERE text LIKE '%¥%' OR text LIKE '%¶%';
