SELECT s.id AS sentence_id, s.text AS sentence_text, e.label AS entity_label, nl.label AS ner_label, nl.description AS ner_description
FROM sentence AS s
JOIN sentence_entity_linking AS sel ON s.id = sel.sentence
JOIN entity AS e ON sel.entity = e.id
JOIN ner_label AS nl ON e.ner_label = nl.id
JOIN language AS lang ON s.language = lang.id
WHERE lang.iso_code = 'sv'
ORDER BY s.id;
