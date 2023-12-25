SELECT d.id AS document_id, COUNT(DISTINCT e.id) AS entity_count
FROM document AS d
JOIN sentence AS s ON d.id = s.document
JOIN sentence_entity_linking AS sel ON s.id = sel.sentence
JOIN entity AS e ON sel.entity = e.id
GROUP BY d.id
ORDER BY d.id;
