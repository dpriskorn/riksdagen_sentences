SELECT COUNT(DISTINCT sel.sentence) AS total_swedish_sentences_with_entity
FROM sentence_entity_linking AS sel
JOIN sentence AS s ON sel.sentence = s.id
JOIN language AS lang ON s.language = lang.id
WHERE lang.iso_code = 'sv';
