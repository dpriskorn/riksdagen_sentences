SELECT COUNT(*) AS total_rows
FROM rawtoken_sentence_linking
JOIN rawtoken ON rawtoken.id = rawtoken_sentence_linking.rawtoken
JOIN language ON rawtoken.language = language.id
WHERE language.iso_code = 'sv';
