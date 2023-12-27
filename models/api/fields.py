# from typing import List
#
# from pydantic import BaseModel, Field
#
#
# class EvolvableListField(BaseModel):
#     type: str = "array"  # default to the most common
#     name: str
#     read_only: bool = False  # default to the most common
#     value: List[str]
#
#
# class EvolvableStringField(BaseModel):
#     type: str = "text"  # default to the most common
#     name: str
#     read_only: bool = False  # default to the most common
#     value: str
#
#
# class EvolvableReadOnlyStringField(EvolvableStringField):
#     read_only: bool = True  # default to the most common
#
#
# class EvolvableUrlField(EvolvableStringField):
#     type: str = "url"
#     read_only: bool = True
#
#
# class Token(EvolvableStringField):
#     name = "token"
#
#
# class LexicalCategoryQid(EvolvableStringField):
#     name = "lexical_category_qid"
#
#
# class SyntacticHeadLid(EvolvableStringField):
#     name = "syntactic_head_lid"
#
#
# class IsoLanguageCode(EvolvableStringField):
#     name = "iso_language_code"
#     value = Field(None, title="ISO 639 language code", max_length=3, min_length=2)
#
#
# class AcceptedLicenseQids(EvolvableListField):
#     name = "accepted_license_qids"
#
#
# class Next(EvolvableUrlField):
#     name = "next"
#
#
# class Information(EvolvableReadOnlyStringField):
#     name = "information"
#     value = (
#         "This is an evolvable JSON API. :)"
#         "It follows the JSON API 1.1 specification except for "
#         "the following requirement because it hinders storing state in the network "
#         "'The members data and errors MUST NOT coexist in the same document.'. "
#         "Usage instructions:\n"
#         "* For tokens with no space we need [lexical_category_qid, iso_language_code].\n"
#         "* For tokens with at least one space (aka phrase) we need "
#         "[lexical_category_qid, iso_language_code, syntactic_head_lexeme_id]\n"
#         "Please remove the errors key from data before resubmitting."
#     )
