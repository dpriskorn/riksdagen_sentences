SELECT rt.text, nt.text AS normtoken_text, ROUND(score.value, 2) AS score_value
FROM rawtoken rt
JOIN rawtoken_sentence_linking rtl ON rt.id = rtl.rawtoken
JOIN sentence s ON rtl.sentence = s.id
JOIN language l ON s.language = l.id
JOIN score ON rt.score = score.id
JOIN rawtoken_normtoken_linking rtnl ON rt.id = rtnl.rawtoken
JOIN normtoken nt ON rtnl.normtoken = nt.id
WHERE l.iso_code = 'sv' AND ROUND(score.value, 2) > 0.7;
