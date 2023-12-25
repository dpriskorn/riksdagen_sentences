SELECT s.id AS sentence_id, COUNT(e.id) AS entity_count
FROM sentence AS s
JOIN sentence_entity_linking AS sel ON s.id = sel.sentence
JOIN entity AS e ON sel.entity = e.id
JOIN language AS lang ON s.language = lang.id
WHERE lang.iso_code = 'sv'
GROUP BY s.id
ORDER BY s.id;
