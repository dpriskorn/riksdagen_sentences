# from unittest import TestCase
#
# import spacy
# from spacy.language import Doc
#
#
# class TestSentence(TestCase):
#     def test_print_ner_result(self):
#         text = """Europa är den enda kontinent som namngivit sig själv.
#         De andra världsdelarna har fått sina namn tilldelade.
#         På tidiga medeltida världskartor delas världen in i tre delar, Europa, Asien och Afrika."""
#         nlp = spacy.load("sv_core_news_lg")
#         doc: Doc = nlp(text)
#         # print(text)
#         print("NER result:")
#         for sentence in doc.sents:
#             print(sentence.text)
#             print(sentence.start)
#             print(sentence.end)
#             for entity in doc.ents:
#                 if entity.start >= sentence.start and entity.end <= sentence.end:
#                     print(f"{entity.text} -> {entity.label_}")
#         # for sent in doc.sents:
#         #     s = Sentence(sent=sent, document=Document(external_id="", dataset_id=0))
#         #     print(s.text)
#         #     s.print_ner_result()
#         # self.fail()
