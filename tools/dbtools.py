import psycopg2
import psycopg2.extras
from psycopg2.extensions import AsIs


class DbToolsPostgres(object):
    def __init__(self, connection_string):
        """
        initialize the class object with a connection string
        :param connection_string: a dictionary with various details to connect to database
        """
        self.conn = connection_string

    def connect(self):
        """
        Iterate over the connection string provided and connect to the DB
        """
        conn = {}
        for k in ['database', 'host', 'port', 'user', 'password']:
            v = self.conn[k]
            conn[k] = v
        conn_obj = psycopg2.connect(**conn)
        conn['connection'] = conn_obj
        self.conn = conn
        return True

    def execute_sql(self, sql, params=None, scalar=False, rowcount=False):
        """
        This will execute the parameterized sql given a dictionary of params for the values against each column
        :param sql: normal/parameterizes sql given to execute
        :param params: parameters provided in case of dynamic sql execution
        :param scalar: in case one value of the given select statement needs to be returned
        :param rowcount: gives deto;s about total rows affected in case of update/insert
        :return: returns rowcount as detailed above
        """
        # Append a trailing semicolon
        if isinstance(sql, str) and sql[-1] != ';':
            sql += ';'
        if params is not None:
            (sql, params) = self.apply_asis(sql, params)
        conn = self.conn['connection']
        # In this case we accept and execute sql with a parameter of Sql
        # This could be solved using pandas but using dictionary is efficient wrt processing and memory utilization
        curs = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        curs.execute(sql, params)
        if curs.description:
            if scalar:
                return curs.fetchone()
            else:
                return curs.fetchall()
        return curs.rowcount if rowcount else None

    def apply_asis(self, sql, params):
        """
        used to convert python data types to postgres understandable datat ypes
        :param sql: given sql
        :param params: given parameters in python
        :return: parameters converted for the purpose of execution in db
        """
        # Apply AsIs to all parameters
        _sql = sql
        params1 = {}
        for key, value in params.items():
            if isinstance(value, str) and "'" in value:
                # In case quoted string is provided, it mught affect the way sql is interpreted
                params1[key] = AsIs(value.replace("'", "''"))
            elif isinstance(value, list):
                # Converting an array to a format DB can understand
                if len(value) == 1:
                    params1[key] = AsIs('(' + str(value)[1:-1] + ')')
                else:
                    params1[key] = AsIs(tuple(value))
            elif isinstance(value, dict):
                # Converting the dictionary provided to a json format
                params1[key] = AsIs(psycopg2.extras.Json(value))
            else:
                # Other data types are sent back as is
                params1[key] = AsIs(value)
        return _sql, params1

    def commit_transaction(self):
        """
        Sends a COMMIT statement to the database to commit all the statements within the current transaction.
        """
        self.conn['connection'].commit()

    def rollback_transaction(self):
        """
        Sends a ROLLBACK statement to the database to ROLLBACK all the statements within the current transaction.
        """
        self.conn['connection'].rollback()

    def close_connection(self):
        """
        Closes the current connection, if a transaction is pending, sends a ROLLBACK statement before closing the connection
        """
        self.conn['connection'].cursor().close()
        self.conn['connection'].close()

