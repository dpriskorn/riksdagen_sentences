from models.crud.database_handler import Mariadb


class Delete(Mariadb):
    # TODO delete rawtokens and normtokens with unaccepted chars
    # TODO delete garbage sentences with unaccepted token chars
    """SELECT *
    FROM sentence s
    WHERE text LIKE '%¥%';
    """
    pass