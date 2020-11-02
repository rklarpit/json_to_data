from services.jsontodataservice import create_load_automated_tables
from tools.dbtools import DbToolsPostgres

DB = DbToolsPostgres(connection_string={'database': 'databse',
                                        'host': 'host',
                                        'port': 'port',
                                        'user': 'user',
                                        'password': 'password'})


def handler(event, context):
    """
    The handler executes when the API is executed with a provided json body
    :param event:
    :param context:
    :return:
    """
    json_data = event.get('body')
    DB.connect()
    try:
        create_load_automated_tables(DB, json_data)
        DB.commit_transaction()
    except Exception as err:
        DB.rollback_transaction()
        print('Error is:', str(err))
    finally:
        DB.close_connection()
