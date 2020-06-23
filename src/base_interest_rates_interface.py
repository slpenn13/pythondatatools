#!/usr/bin/python3
""" Basic Interface to FRED"""
import sys
import pandas as pd
import numpy as np
import debug_control as dbc
import mysql_db_class as dbsql
import sql_class_base as sbc
import backup_utility as bu


class base_rates_db_interface():
    """ Base class too manage construction and insertion of Rate data """
    def __init__(self, options, dryrun):
        self.mysql_conn = None
        if options is not None and isinstance(options, dict):
            self.insert_query = None
            self.current_view_query = None

            self.options = options.copy()
            self.dbg, self.print_dbg = bu.calc_debug_levels(self.options)

            if "path" in options.keys() and "group" in options.keys():
                self.mysql_conn = dbsql.mysql_db_class(path=options["path"],
                                                       group=options["group"], password=None)
                if self.mysql_conn is None:
                    raise ValueError("Unable to connect to SQL Server (path)")
            elif "password" in options and options["password"] != "":
                self.mysql_conn = dbsql.mysql_db_class(user=options["user"],
                                                       password=options["password"],
                                                       host=options["db_host_ip"], db="Investing")

                if self.mysql_conn is None:
                    raise ValueError("Unable to connect to SQL Server (user + passw)")
            elif dryrun:
                dbc.error_helper("Warning -- running w dryrun", dbg=self.dbg)

            else:
                raise ValueError("Options dict does not meet requirements")

            if "table" in self.options.keys() and "index_name" in self.options.keys():
                self.insert_query = sbc.sql_query_base(self.options, q_str="INSERT")

            else:
                raise ValueError("Options must include write table && index_name")

            if "current_view" in self.options.keys() and "index_name" in self.options.keys():
                self.current_view_query, _ = sbc.calculate_current_view(
                    self.options["current_view"], self.options["index_name"])

            else:
                if self.print_dbg:
                    dbc.print_helper("Warning -- no current_view", dbg=self.dbg)

            self.options["exclude_perc"] = (float(self.options["exclude_perc"])
                                            if "exclude_perc" in self.options.keys() else 0.295)

        else:
            raise ValueError("Options must be of type dictionary")

    def __del__(self):
        if self.mysql_conn:
            del self.mysql_conn

        if isinstance(self.dbg, dbc.debug_control):
            self.dbg.close()

    def construct_db_insert(self, df):
        """ Constructs SQL statement from either data frame or dict(ionary)"""
        try:
            build_status = -1
            if isinstance(df, pd.DataFrame) and df.shape[0] >= 1:
                build_status = self.db_dataframe_insert(df)
            elif isinstance(df, dict):
                build_status = self.db_dict_insert(df)
            else:
                raise ValueError("Error (construct_db_insert)")

            if self.mysql_conn is not None and build_status == 0:
                if self.dbg:
                    self.insert_query.print_q_str("SQL \n", dbg=self.dbg)

                success = self.mysql_conn.insert(self.insert_query.get_query())
                dbc.print_helper(("SQL: construct_db_insert " + str(success)), dbg=self.dbg)
            else:
                self.insert_query.print_q_str("construct_db_insert--failed", dbg=self.dbg)
        except ValueError:
            dbc.error_helper("Failure", None, None, dbg=self.dbg)
        else:
            dbc.error_helper(("Failure" + str(sys.exc_info()[0])), None,
                             "construct_db_insert", dbg=self.dbg)

    def db_dataframe_insert(self, df):
        """ Constructs SQL insert from DataFRame"""
        build_status = -1
        if isinstance(df, pd.DataFrame) and not df.empty and self.insert_query:
            self.insert_query.construct_insert_start(include_index=True)
            self.insert_query.append_names(excludes=self.options["index_name"], append="")

            row_width = df.shape[1]
            row_count = 1
            first_write = True
            # print_dbg = dbc.test_dbg(self.dbg)
            print_dbg = False

            for row in df.iterrows():
                if  np.all(np.isnan(row[1])):
                    dbc.print_helper(("Excluding " + str(row[0])), dbg=self.dbg)
                elif np.isnan(row[1]).sum() / float(df.shape[1]) > self.options["exclude_perc"]:
                    dbc.print_helper((" ".join(["Excluding (data @",
                                                str(self.options["exclude_perc"]), ")",
                                                str(row[0])])), dbg=self.dbg)
                else:
                    base = "('" + convert_timestamp(row[0]) + "', "
                    j = 1

                    insert = ", "
                    for i in row[1]:
                        if j == row_width:
                            insert = ")"
                        if np.isnan(i):
                            base = base + 'NULL' + insert
                        else:
                            base = base + str(i) + insert
                        j = j + 1

                    if first_write:
                        insert = "\n"
                        first_write = False
                    else:
                        insert = ", \n"

                    if print_dbg:
                        dbc.print_helper((str(row_count) + " " + base), dbg=self.dbg)
                    self.insert_query.append_query_element(base, append=insert)

                row_count = row_count + 1

            self.insert_query.clean_query_element()
            build_status = 0
        return build_status

    def db_dict_insert(self, df):
        """ Constructs SQL insert from python dictionary"""
        build_status = -1
        if isinstance(df, dict) and not df.empty and self.insert_query:
            dict_len = len(df)
            dict_cnt = 1
            base_k = ""
            base_v = ""
            if self.insert_query and self.insert_query.index_name in df:
                self.insert_query.construct_insert_start(True)  # True=> include index_name
                base_v = "'" + df[self.insert_query.index_name]  + "', "
            else:
                raise ValueError("Faulty Type (db_dict_insert) insert data")

            for key, val in df.items():
                if key != self.insert_query.index_name:
                    if dict_cnt == dict_len:
                        base_k = base_k + key + ") VALUES ("
                        base_v = base_v + str(val) + ");"
                    else:
                        base_k = base_k + key + ", "
                        base_v = base_v + str(val) + ", "
                dict_cnt = dict_cnt + 1

            self.insert_query.append_query_element(base_k, "")
            self.insert_query.append_query_element(base_v, "")
            build_status = 0
        else:
            raise ValueError("Faulty Type (db_dict_insert)")
        return build_status

    def db_vertical_insert(self, df):
        """ Insert values into veritcal (tim-series) table"""
        if isinstance(df, pd.DataFrame) and not df.empty and self.insert_query:
            self.insert_query.construct_insert_start(True)
            append = ", "
            base_v = ""

            self.insert_query.append_names(self.options["index_name"], append="")
            cols_final = len(self.insert_query.columns)

            fld = "%s"
            for j in range(1, cols_final+1):
                if j == cols_final:
                    append = ");"

                prepend = "(" if j == 1 else ""
                base_v = prepend + base_v + fld + append

            self.insert_query.append_query_element(base_v, append="")
            self.insert_query.print_q_str("db_vertical init", dbg=self.dbg)
            vals = []

            for row in df.iterrows():
                dt_val = row[0] if isinstance(row[0], str) else convert_timestamp(row[0])

                if np.isnan(row[1][0]):
                    dbc.print_helper(("Excluding: " + dt_val), dbg=self.dbg)
                else:
                    res = (dt_val, df.columns[0], str(row[1][0]), self.options["source"])
                    vals.append(res)

            if self.mysql_conn is not None:
                success = self.mysql_conn.insert_mutiple_rows(
                    self.insert_query.get_query(), vals)
                dbc.print_helper(("SQL: db_vertical_insert " + str(success)), dbg=self.dbg)
            else:
                dbc.print_helper(("SQL " + self.insert_query.get_query()), dbg=self.dbg)
                print(vals)

    def calc_most_recent_date(self, current_max=None):
        ''' Given current max date as string (in %Y-%m-%d format) & current max query determine max
            writeable date
        '''
        ret_value = None

        if self.current_view_query:
            determine_periodicity = False

            if self.current_view_query.sql_type_ind is sbc.sql_type.SELECT:
                result = self.mysql_conn.query(self.current_view_query.get_query())
            elif  self.current_view_query.sql_type_ind is sbc.sql_type.STORED_PROCEDURE_RES:
                result = self.mysql_conn.execute_stored_procedure_result(
                    self.current_view_query.get_query(),
                    self.current_view_query.vars)

                determine_periodicity = True
            else:
                result = None

            if result and current_max is None:
                dteq = self.extract_db_result_value(result)
                ret_value = bu.dt.datetime.strftime(dteq, "%Y-%m-%d")

            elif result and current_max is not None:
                dte = bu.dt.datetime.strptime(current_max, "%Y-%m-%d")
                dteq = self.extract_db_result_value(result)
                if determine_periodicity:
                    statement_frequency = self.extract_db_result_value(
                        result, (len(result[0])-1))
                    one_day = bu.calc_single_period_advance(
                        dteq.month, dteq.year, statement_frequency)

                else:
                    one_day = bu.dt.timedelta(days=1)

                if dteq >= dte:
                    dteq = dteq + one_day
                    ret_value = bu.dt.datetime.strftime(dteq, "%Y-%m-%d")
                else:
                    ret_value = current_max
            else:
                ret_value = current_max
        else:
            ret_value = current_max

        return ret_value

    def extract_db_result_value(self, result, position=None):
        """ Simple method for extracting results from query / SP results based on position or
            Index_name
        """
        if isinstance(result, list) and position is not None:
            value = result[0][position]
        elif isinstance(result, list) and "location" in self.options["current_view"]:
            value = result[0][self.options['current_view']['location']]
        else:
            value = result[0][self.options['index_name']]

        return value

def convert_timestamp(val, split="-"):
    """ Converts pandas TimeSta,mp into a date str"""
    res = ""
    if isinstance(val, pd.Timestamp):
        dt2 = val.date()
    elif isinstance(val, str):
        dt2 = ""
        if val.find(split) == 4:
            dt2 = bu.dt.datetime.strptime(val, split.join(["%Y", "%m", "%d"]))
        elif val.find("/") == 4:
            dt2 = bu.dt.datetime.strptime(val, "/".join(["%Y", "%m", "%d"]))
        else:
            raise ValueError("String must be in %Y-%m-%d or %Y/%m/%d format")
    else:
        raise ValueError("Required Type TimeStamp " + str(type(val)))

    mnth = str(dt2.month) if dt2.month > 9 else "0" + str(dt2.month)
    day = str(dt2.day) if dt2.day > 9 else "0" + str(dt2.day)
    res = split.join([str(dt2.year), mnth, day])

    return res
