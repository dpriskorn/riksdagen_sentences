SELECT rawtoken.id
FROM rawtoken
JOIN lexical_category ON rawtoken.lexical_category = lexical_category.id
JOIN language ON rawtoken.language = language.id
WHERE rawtoken.text = 'hem' 
AND lexical_category.qid  = 'Q1084'
AND language.iso_code = 'sv';
