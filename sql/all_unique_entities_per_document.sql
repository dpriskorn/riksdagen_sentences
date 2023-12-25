SELECT d.id AS document_id, e.label AS entity_label, nl.label AS ner_label, nl.description AS ner_description
FROM document AS d
JOIN sentence AS s ON d.id = s.document
JOIN sentence_entity_linking AS sel ON s.id = sel.sentence
JOIN entity AS e ON sel.entity = e.id
JOIN ner_label AS nl ON e.ner_label = nl.id
GROUP BY d.id, e.label, nl.label, nl.description
ORDER BY d.id, nl.label;