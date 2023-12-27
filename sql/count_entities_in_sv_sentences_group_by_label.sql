SELECT COUNT(*) AS entity_count, ner_label.label
FROM sentence_entity_linking AS sel
JOIN sentence AS s ON sel.sentence = s.id
JOIN language AS lang ON s.language = lang.id
JOIN entity ON sel.entity = entity.id
JOIN ner_label ON entity.ner_label = ner_label.id
WHERE lang.iso_code = 'sv'
GROUP BY ner_label.id
ORDER BY entity_count DESC;
