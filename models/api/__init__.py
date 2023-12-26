import logging
from typing import List, Dict, Any, Tuple

from fastapi import FastAPI, APIRouter
from fastapi.responses import JSONResponse
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi

from models.api.sentence_result import SentenceResult
from models.crud.read import Read

app = FastAPI()
router = APIRouter()
app.include_router(router)

logger = logging.getLogger(__name__)


def supported_lexical_language_qids() -> List[str]:
    read = Read()
    read.connect_and_setup()
    return read.get_all_lexical_language_qids()


def supported_license_qids() -> List[str]:
    read = Read()
    read.connect_and_setup()
    return read.get_license_qids()


def supported_iso_codes() -> List[str]:
    read = Read()
    read.connect_and_setup()
    return read.get_all_iso_codes()


def lookup_sentences(
    iso_language_code: str,
    lexical_category_qid: str,
    token: str,
    # accepted_license_qids: List[str],
    # syntactic_head_lid: str,
) -> Tuple[int, List[SentenceResult]]:
    # TODO we get back a list of tuples with the raw sentence and uuid
    # TODO implement limit and offset
    # TODO implement filtering based on license qid
    read = Read()
    read.connect_and_setup()
    if is_compound_token(token=token):
        logger.info("Got compund token")
        """This is complicated because Wikidata does NOT store
        all possible forms of phrases currently so we need to
        lookup the forms of the syntactic head to be sure we get
        as many sentences as possible"""
        # todo make query that takes accepted_license_qids and syntactic_head_lid into account
        # todo lookup all lexeme forms using WBI
        # todo get all rawtoken_ids corresponding to these forms
        # todo for each rawtoken_id get sentences matching that and the string of rest of the token
        # todo for each syntactic lexeme form run compound_token_without_syntactic_head() find the shortest token without syntactic head

        # For now we just search for the whole compound token in the sentences
        # Since we don't know the syntactic head we cannot support separable verb phrases
        data = read.get_sentences_for_compound_token(
            compound_token=token, language=iso_language_code
        )
        read.close_db()
        return data
    else:
        logger.info("Got simple token")
        rawtoken_id = read.get_rawtoken_id_with_specific_language_and_lexical_category(
            rawtoken=token,
            lexical_category=lexical_category_qid,
            language=iso_language_code,
        )
        if rawtoken_id:
            data = read.get_sentences_for_rawtoken_without_space(
                rawtoken_id=rawtoken_id
            )
            read.close_db()
            return data
        else:
            return 0, list()
            # raise NotFoundError("rawtoken not found in the database")


# def generate_next_results_url():
#     # Your logic to generate the URL for next results (pagination)
#     pass


def compound_token_without_syntactic_head(token: str, syntactic_head: str) -> str:
    return token.replace(syntactic_head, "").strip()


def is_compound_token(token: str) -> bool:
    """We define a compund token as a token that
    contains at least one space"""
    return len(token.split()) > 1


def serialize(data):
    """This is needed to dump the pydantic model recursively"""
    serialized_data = []
    for item in data:
        serialized_data.append(item.dump_model())
    return serialized_data


# class Data(BaseModel):
#     pass


default_data = {
    "token": {"type": "text", "name": "token", "read-only": False, "value": ""},
    "lexical_category_qid": {
        "type": "text",
        "name": "lexical_category_qid",
        "read-only": False,
        "value": "",
    },
    # "syntactic_head_lid": {
    #     "type": "text",
    #     "name": "syntactic head wikidata lexeme id",
    #     "read-only": False,
    #     "value": "",
    # },
    "iso_language_code": {
        "type": "text",
        "name": "iso_language_code",
        "read-only": False,
        "value": "",
    },
    # "accepted_license_qids": {
    #     "type": "array",
    #     "name": "accepted_license_qids",
    #     "read-only": False,
    #     "value": [],
    # },
    # "next": {"type": "url", "name": "next", "read-only": True, "value": ""},
    "information": {
        "type": "text",
        "name": "information",
        "read-only": True,
        "value": (
            "This is an evolvable JSON API. :)"
            "It follows the JSON API 1.1 specification except for "
            "the following requirement because it hinders storing state in the network "
            "'The members data and errors MUST NOT coexist in the same document.'. "
            "Usage instructions:\n"
            "* For tokens with no space we need [token, lexical_category_qid, iso_language_code].\n"
            "* For tokens with at least one space (aka phrase) we need "
            "[token, iso_language_code]\n"
            "Please remove the errors key from data before resubmitting."
        ),
    },
}


@app.post("/lookup")
async def lookup(body: Dict[str, Any]):
    logger.info("Got lookup")
    error_messages = []
    global default_data

    if isinstance(body, dict) and body.get("data"):
        data = body["data"]
        # Get (or setup missing) fields
        ## strings
        if data.get("token") and data.get("token").get("value"):
            token = data["token"]["value"]
        else:
            token = ""
            data["token"] = default_data["token"]
        if data.get("lexical_category_qid") and data.get("lexical_category_qid").get(
            "value"
        ):
            lexical_category_qid = data["lexical_category_qid"]["value"]
        else:
            lexical_category_qid = ""
            data["lexical_category_qid"] = default_data["lexical_category_qid"]
        if data.get("iso_language_code") and data.get("iso_language_code").get("value"):
            iso_language_code = data["iso_language_code"]["value"]
        else:
            iso_language_code = ""
            data["iso_language_code"] = default_data["iso_language_code"]
        # if data.get("syntactic_head_lid") and data.get("syntactic_head_lid").get(
        #     "value"
        # ):
        #     syntactic_head_lid = data["syntactic_head_lid"]["value"]
        # else:
        #     syntactic_head_lid = ""
        #     data["syntactic_head_lid"] = default_data["syntactic_head_lid"]
        ## lists
        # if data.get("accepted_license_qids") and data.get("accepted_license_qids").get(
        #     "value"
        # ):
        #     accepted_license_qids = data["accepted_license_qids"]["value"]
        # else:
        #     accepted_license_qids = list()
        #     data["accepted_license_qids"] = default_data["accepted_license_qids"]

        if (
            not token.strip()
        ):  # Checks if token is an empty string or contains only whitespace
            error_message = "Token cannot be empty."
            error_messages.append(error_message)

        # if is_compound_token(token=token):
        # disabled because of complexity
        # It's a phrase, extract the lexeme ID for the syntactic head of the phrase
        # syntactic_head_lid = data["syntactic_head_lid"]["value"]

        # if syntactic_head_lid_needed and not syntactic_head_lid:
        #     error_message = (
        #         "syntactic_head_lid cannot be empty when there is a space in the token.\n"
        #     )
        #     error_messages.append(error_message)

        # if accepted_license_qids is not None:
        #     # Verify if all provided license QIDs are valid
        #     valid_license_qids = [
        #         qid for qid in accepted_license_qids if qid in supported_license_qids()
        #     ]
        #     invalid_license_qids = [
        #         qid for qid in accepted_license_qids if qid not in valid_license_qids
        #     ]
        #     if invalid_license_qids:
        #         error_message = (
        #             f"Invalid license QIDs: {', '.join(invalid_license_qids)}"
        #         )
        #         error_messages.append(error_message)
        #     else:
        #         # todo supply structured options here
        #         # data["accepted_license_qids"]["value"] = valid_license_qids
        #         pass

        # Validate provided lexical_category_qid against accepted QIDs
        invalid_iso_code = iso_language_code not in supported_iso_codes()
        if invalid_iso_code:
            # todo supply structured options here
            error_message = f"Invalid ISO code: '{iso_language_code}'. Supported codes: {', '.join(supported_iso_codes())}"
            error_messages.append(error_message)

        # Validate provided lexical_category_qid against accepted QIDs
        if not is_compound_token(token=token):
            invalid_qid = lexical_category_qid not in supported_lexical_language_qids()
            if invalid_qid:
                # todo supply structured options here
                error_message = f"Invalid QID: {lexical_category_qid} for lexical_category_qid. Supported QIDs: {', '.join(supported_lexical_language_qids())}"
                error_messages.append(error_message)

        # Generate the URL for next results (pagination)
        # next_url = generate_next_results_url()
        # data["next"]["value"] = next_url  # Assuming next_url is generated here
    else:
        body["data"] = default_data
        error_message = f"We expect a JSON object"
        error_messages.append(error_message)
        # Please the linter
        lexical_category_qid = token = iso_language_code = ""
        accepted_license_qids = list()

    # Setup errors and return
    if error_messages:
        body["errors"] = error_messages
    if not body.get("errors"):
        # We are good to go!
        count, data = lookup_sentences(
            lexical_category_qid=lexical_category_qid,
            token=token,
            # accepted_license_qids=accepted_license_qids,
            # syntactic_head_lid=syntactic_head_lid,
            iso_language_code=iso_language_code,
        )
        data = serialize(data)
        headers = {"X-Total-Count": f"{count}"}
        # Recommended to be an int here https://stackoverflow.com/questions/3715981/what-s-the-best-restful-method-to-return-total-number-of-items-in-an-object
        # headers = {"X-Total-Count": count}
        return JSONResponse(content={"data": data}, headers=headers)
    else:
        # Return the updated request data to keep all state in the network
        return body


for route in app.routes:
    print(route)


@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    return get_swagger_ui_html(openapi_url="/openapi.json", title="Your API - FastAPI")


@app.get("/openapi.json", include_in_schema=False)
async def get_open_api_endpoint():
    return get_openapi(
        title="Your API - FastAPI",
        version="1.0.0",
        description="This is a fantastic API made with FastAPI.",
        routes=app.routes,
    )
