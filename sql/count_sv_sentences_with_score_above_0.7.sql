SELECT COUNT(s.id) AS num_sentences_with_high_score_tokens
FROM sentence s
JOIN language l ON s.language = l.id
JOIN score sc ON s.score = sc.id
WHERE l.iso_code = 'sv' AND sc.value > 0.7;
