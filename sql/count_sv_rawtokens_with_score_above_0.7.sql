SELECT COUNT(rt.id) AS num_high_score_tokens
FROM rawtoken rt
JOIN language l ON rt.language = l.id
JOIN score s ON rt.score = s.id
WHERE l.iso_code = 'sv' AND ROUND(s.value, 2) > 0.7;
