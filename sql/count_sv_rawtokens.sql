SELECT COUNT(rt.text) AS num_tokens
FROM rawtoken rt
JOIN rawtoken_sentence_linking rtl ON rt.id = rtl.rawtoken
JOIN sentence s ON rtl.sentence = s.id
JOIN language l ON s.language = l.id
WHERE l.iso_code = 'sv';
