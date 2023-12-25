SELECT COUNT(s.text) AS num_sentences
FROM sentence s
JOIN language l ON s.language = l.id
WHERE l.iso_code = 'sv';
