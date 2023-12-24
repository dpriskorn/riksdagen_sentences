SELECT s.text
FROM sentence s
JOIN language l ON s.language = l.id
WHERE l.iso_code = 'sv'
AND LOWER(s.text) LIKE '% skatt %';
