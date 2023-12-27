SELECT sentence.text, sentence.uuid
FROM sentence
JOIN language ON sentence.language = language.id
WHERE language.iso_code = 'sv'
AND LOWER(sentence.text) LIKE LOWER('%ett land%')
ORDER BY LENGTH(sentence.text) ASC;
