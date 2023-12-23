import logging
from datetime import datetime

# not_accepted_languages = {"ko", "ceb", "jv", "sh", "is", "ms", "nds", "nl", "sr", "vi", "sk", "ca"}
accepted_languages = {"sv", "en", "nb", "de", "fr"}
fasttext_model = "lid.176.bin"
fasttext_model_download_date = datetime.strptime("2023-12-21", "%Y-%m-%d").date()
loglevel = logging.INFO
