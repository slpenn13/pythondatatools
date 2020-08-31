""" Class wrapper around the python interface to mysql database """
#!/usr/bin/python3
# import MySQLdb as mysqldb
import sys
import pandas as pd
import sqlalchemy as sa 
from sqlalchemy import create_engine
from sqlalchemy import inspect


class sqlalchemy_db_class():
    """ Simple class wrapping access to mysql database """

    def __init__(self, path="/home/spennington/data/init_db.db", group="remote", password=None,
                 host=None, user="spennington", db="jobsearch", dbg=False):
        self.connection = None
        self.engine = None
        self.host = host
        self.user = user
        self.database = db
        self.port = 3306
        self.dbg = dbg

        if password and host:
            if self.dbg:
                print("Warning -- not currently supported")
            # self.connection = mysqldb.connect(option_files=path, option_groups=group,
            #                                  use_unicode=True, charset="utf8",
            #                                  collation="utf8_general_ci",
            #                                  use_pure=True,
            #                                  db=self.database)
        else:
            if password is None and path and isinstance(path, str) and\
                    path.endswith(".db"):
                
                dbi_uri = "///".join(["sqlite:", path])

                if self.dbg:
                    print(sa.__version__)
                    print(dbi_uri)
                self.engine = create_engine(dbi_uri)

                if self.engine and isinstance(self.engine, sa.engine.base.Engine):
                    if self.dbg:
                        inspector = inspect(self.engine)
                        print(inspector.get_table_names())
                    #self.user = conf['user']
                    #self.host = conf['host']
                    #password = conf['password'].replace('"', '')
                    #if 'port' in conf.keys():
                    #    self.port = conf['port']

                else:
                    raise ValueError("There was a problem with database file")
            # conf['password'] = conf['password'].replace("\"", '')
            # conf['password'] = conf['password'].encode(encoding='utf-8')

    def insert(self, data, params_tuple=None):
        """ Simple insert query -- with roll back in case of failure , returns 1
            Note to write multiple instancas pass a pd.DataFrame
            in case of success 0
            EX:         "INSERT INTO securities (cusip, BBG_Name, tranche, vintage, classification)
            EX (cont.):      VALUES ();
        """
        success = 1
        with self.engine.connect() as conn:
            with conn.begin() as trans:
                try:
                    result = None
                    meta = sa.MetaData(self.engine)

                    if isinstance(data, str) and params_tuple is None:
                        print("Warning -- SQL injection -- candidate (insert)")
                        result = self.engine.execute(data)
                    elif isinstance(data, pd.DataFrame) and data.shape[0] > 0 and\
                            params_tuple and isinstance(params_tuple, dict):
                        table = sa.Table(params_tuple['table'], meta, autoload=True,
                                         autoload_with=self.engine)
                        result = conn.execute(table.insert(), data.to_dict(orient='records'))
                        # print("DF " + str(result))        
                    elif isinstance(data, pd.Series) and\
                            params_tuple and isinstance(params_tuple, dict):
                        table = sa.Table(params_tuple['table'], meta, autoload=True,
                                         autoload_with=self.engine)
                        vals = data.to_dict()
                        # ins = table.insert(values=vals)
                        result = conn.execute(table.insert(), vals)
                        # print("Series here " + str(result)) # + str(ins))
                    else:
                        if self.dbg:
                            print(type(data))
                        raise ValueError("SQL (query) type combination not supported")

                    success = 0
                    trans.commit()
                except ValueError as v:
                    print("Failed Insert {}".format(v))
                except sa.exc.IntegrityError as e:
                    print(e)
                except sa.exc.StatementError as s:
                    print(s.statement, " ", s.params)
                    print(s)
                except sa.exc.DBAPIError as d:
                    print(d.StatementError.statement, " ", d.StatementError.params)
                    print(d)
                except sa.exc.SQLAlchemyError as g:
                    print(g)
                except:
                    print("Failed Insert: ", sys.exc_info()[0])
                    # self.engine.rollback()
                finally:
                    conn.close()
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
        except sa.exc.SQLAlchemyError as err:
            print("Failed Insert: {}".format(err))
            self.connection.rollback()

        return success

    def query(self, query, params_tuple=None):
        """ Select query fetch -- applies MySQLCursorDict to cursor"""

        result = None
        with self.engine.connect() as conn:
            try:
                result = None
                meta = sa.MetaData(self.engine).reflect()

                if query and isinstance(query, str) and params_tuple is None:
                    print("Warning -- SQL injection -- candidate (insert)")
                    result = self.engine.execute(query)

                elif query is None and isinstance(params_tuple, dict) and\
                        'table' in params_tuple.keys() and len(params_tuple) == 1:
                    table = meta.tables[params_tuple['table']]

                    select_str = sa.select([table])
                    result = self.engine.execute(select_str) 
                else:
                    raise ValueError("SQL (query) type combination not supported")

            except ValueError as v:
                print("Failed Insert {}".format(v))
            except sa.exc.IntegrityError as e:
                print(e)
            except sa.exc.StatementError as s:
                print(s.statement, " ", s.params)
                print(s)
            except sa.exc.DBAPIError as d:
                print(d.StatementError.statement, " ", d.StatementError.params)
                print(d)
            except sa.exc.SQLAlchemyError as g:
                print(g)
            except:
                print("Failed Insert: ", sys.exc_info()[0])
            finally:
                conn.close()
        return result 

    def execute_stored_procedure(self, sp_name, sp_args_list):
        """ Call stored procedure from mysql"""
        success = 1
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.callproc(sp_name, sp_args_list)
            self.connection.commit()
            cursor.close()
            success = 0
        except sa.exc.SQLAlchemyError as err:
            print("Failed to exceute stored procedure: {}".format(err))
            self.connection.rollback()

        return success

    def execute_stored_procedure_result(self, sp_name, sp_args_list):
        """ Call stored procedure from mysql"""
        success = 1
        try:
            cursor = self.connection.cursor(dictionary=True)
            success = cursor.callproc(sp_name, sp_args_list)

            # print(cursor.column_names) -- columns name not useful
            for result in cursor.stored_results():
                success = result.fetchall()
            # print(success)
            # self.connection.commit()
            cursor.close()
        except sa.exc.SQLAlchemyError as err:
            print("Failed to exceute stored procedure: {}".format(err))
            self.connection.rollback()

        return success


    def __del__(self):
        if self.connection:
            self.connection.close()
