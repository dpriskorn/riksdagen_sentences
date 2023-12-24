SELECT rt.text, score.value AS score_value
FROM rawtoken rt
JOIN rawtoken_sentence_linking rtl ON rt.id = rtl.rawtoken
JOIN sentence s ON rtl.sentence = s.id
JOIN language l ON s.language = l.id
JOIN score ON rt.score = score.id
WHERE l.iso_code = 'sv' AND ROUND(score.value, 2) > 0.7;
