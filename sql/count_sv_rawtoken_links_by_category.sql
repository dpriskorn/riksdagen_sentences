SELECT COUNT(rawtoken.lexical_category) AS lexical_categories, lexical_category.postag
FROM rawtoken_sentence_linking
JOIN rawtoken ON rawtoken.id = rawtoken_sentence_linking.rawtoken
JOIN language ON rawtoken.language = language.id
JOIN lexical_category ON rawtoken.lexical_category = lexical_category.id
WHERE language.iso_code = 'sv'
GROUP BY lexical_category.postag
ORDER BY lexical_categories DESC;
