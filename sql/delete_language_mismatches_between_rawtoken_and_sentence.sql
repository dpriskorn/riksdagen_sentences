DELETE FROM rawtoken_sentence_linking
WHERE rawtoken_sentence_linking.rawtoken IN (
    SELECT rawtoken.id
    FROM rawtoken
    JOIN rawtoken_sentence_linking ON rawtoken.id = rawtoken_sentence_linking.rawtoken
    JOIN sentence ON rawtoken_sentence_linking.sentence = sentence.id
    JOIN language AS language_rawtoken ON rawtoken.language = language_rawtoken.id
    JOIN language AS language_sentence ON sentence.language = language_sentence.id
    WHERE language_rawtoken.id <> language_sentence.id
);
