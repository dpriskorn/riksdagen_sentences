SELECT SUM(entity_count) AS total_entities_found
FROM (
    SELECT COUNT(e.id) AS entity_count
    FROM sentence_entity_linking AS sel
    JOIN entity AS e ON sel.entity = e.id
) AS subquery;
