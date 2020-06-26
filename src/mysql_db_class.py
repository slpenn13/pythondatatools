""" Class wrapper around the python interface to mysql database """
#!/usr/bin/python3
# import MySQLdb as mysqldb
import mysql.connector as mysqldb
from mysql.connector import errorcode
import myloginpath


class mysql_db_class():
    """ Simple class wrapping access to mysql database """

    def __init__(self, path="/home/spennington/.mylogin.cnf", group="remote", password=None,
                 host="localhost", user="spennington", db="jobsearch"):
        self.connection = None
        self.host = host
        self.user = user
        self.database = db
        self.port = 3306

        if password is None and path and isinstance(path, str) and\
                path.find('mylogin.cnf') < 0:
            self.connection = mysqldb.connect(option_files=path, option_groups=group,
                                              use_unicode=True, charset="utf8",
                                              collation="utf8_general_ci",
                                              use_pure=True,
                                              db=self.database)
        else:
            if password is None and path and isinstance(path, str) and\
                path.find('mylogin.cnf') >= 0:
                conf = myloginpath.parse(group)

                if conf and isinstance(conf, dict):
                    print(conf)
                    self.user = conf['user']
                    self.host = conf['host']
                    password = conf['password'].replace('"', '')
                    if 'port' in conf.keys():
                        self.port = conf['port']

                else:
                    raise ValueError("There was a problem with .mylogin.cnf")
            # conf['password'] = conf['password'].replace("\"", '')
            # conf['password'] = conf['password'].encode(encoding='utf-8')
            try:
                self.connection = mysqldb.connect(
                    user=self.user, host=self.host, password=password,
                    port=self.port, use_pure=False, ssl_disabled=True, db=self.database)
            except mysqldb.Error as err:
                if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                    print('(1): ')
                elif err.errno == errorcode.ER_BAD_DB_ERROR:
                    print('(2): ')
                else:
                    print('U:  ' + str(err.errno))
                print(err)
                raise ValueError("Failed to connect to MySQL DB")
        self.cursor = self.connection.cursor()

        if self.database is not None:
            self.cursor.execute("USE " + self.database + ";")

    def insert(self, query, params_tuple=None):
        """ Simple insert query -- with roll back in case of failure , returns 1
            in case of success
        """
        success = 1
        try:
            cursor = self.connection.cursor()
            if isinstance(query, str) and params_tuple is None:
                print("Warning -- SQL injection -- candidate (insert)")
                cursor.execute(query)
            elif isinstance(query, str) and isinstance(params_tuple, (tuple, dict)):
                cursor.execute(query, params_tuple)
            elif isinstance(query, tuple) and isinstance(params_tuple, (tuple, dict)):
                cursor.execute(query, params_tuple)
            else:
                raise ValueError("SQL (query) type combination not supported")

            self.connection.commit()
            success = 0
        except ValueError as v:
            print("Failed Insert {}".format(v))
        except mysqldb.Error as err:
            print("Failed Insert: {}".format(err))
            self.connection.rollback()

        return success

    def insert_mutiple_rows(self, query, vals):
        """ Simple insert many rows query -- with roll back in case of failure , returns 1
            in case of success
        """
        success = 1
        try:
            self.cursor.executemany(query, vals)
            self.connection.commit()
            success = 0
        except mysqldb.Error as err:
            print("Failed Insert: {}".format(err))
            self.connection.rollback()

        return success

    def update(self, query, params_tuple=None):
        """ Simple update query -- with roll back in case of failure"""
        success = 1
        try:
            if isinstance(query, str) and params_tuple is None:
                print("Warning -- SQL injection -- candidate (update)")
                self.cursor.execute(query)
            elif isinstance(query, tuple) and isinstance(params_tuple, (tuple, dict)):
                self.cursor.execute(query, params_tuple)
            elif isinstance(query, str) and isinstance(params_tuple, (tuple, dict)):
                self.cursor.execute(query, params_tuple)
            else:
                raise ValueError("SQL (query) type combination not supported")

            self.connection.commit()
            success = 0
        except ValueError as v:
            print("Failed Insert {}".format(v))
        except mysqldb.Error as err:
            print("Failed Insert: {}".format(err))
            self.connection.rollback()

        return success

    def query(self, query, params_tuple=None):
        """ Select query fetch -- applies MySQLCursorDict to cursor"""

        cursor = self.connection.cursor(dictionary=True)
        if isinstance(query, str) and params_tuple is None:
            # print("Warning -- SQL injection -- candidate (query)")
            cursor.execute(query)
        elif isinstance(query, tuple) and isinstance(params_tuple, (tuple, dict)):
            cursor.execute(query, params_tuple)
        elif isinstance(query, str) and isinstance(params_tuple, (tuple, dict, list)):
            cursor.execute(query, params_tuple)
        else:
            base = "SQL (query) type combination not supported %s %s"
            base = base % (str(type(query)), str(type(params_tuple)))
            raise ValueError(base)

        return cursor.fetchall()

    def execute_stored_procedure(self, sp_name, sp_args_list):
        """ Call stored procedure from mysql"""
        success = 1
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.callproc(sp_name, sp_args_list)
            self.connection.commit()
            cursor.close()
            success = 0
        except mysqldb.Error as err:
            print("Failed to exceute stored procedure: {}".format(err))
            self.connection.rollback()

        return success

    def execute_stored_procedure_result(self, sp_name, sp_args_list):
        """ Call stored procedure from mysql"""
        success = 1
        try:
            cursor = self.connection.cursor(dictionary=True)
            success = cursor.callproc(sp_name, sp_args_list)
            for result in cursor.stored_results():
                success = result.fetchall()
            # print(success)
            # self.connection.commit()
            cursor.close()
        except mysqldb.Error as err:
            print("Failed to exceute stored procedure: {}".format(err))
            self.connection.rollback()

        return success


    def __del__(self):
        if self.connection:
            self.connection.close()
