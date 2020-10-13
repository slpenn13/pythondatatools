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
                                                       group=options["group"], password=None,
                                                       db="Investing")
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

            # print_dbg = dbc.test_dbg(self.dbg)
            not_triggered = True
            print_dbg = False

            for i, row in enumerate(df.iterrows()):
                if  np.all(np.isnan(row[1])):
                    dbc.print_helper(("Excluding " + str(row[0])), dbg=self.dbg)
                elif np.isnan(row[1]).sum() / float(df.shape[1]) > self.options["exclude_perc"]:
                    dbc.print_helper((" ".join(["Excluding (data @",
                                                str(self.options["exclude_perc"]), ")",
                                                str(row[0])])), dbg=self.dbg)

                else:
                    arr = row[1].to_numpy(copy=True)
                    mn = np.mean(arr, axis=0)
                    var = np.var(arr, axis=0)

                    # TODO start coding here
                    if mn < 0.0001 and var < 0.0001:
                        dbc.print_helper(("Excluding (data @ {} {} {})".format(
                            mn, var, str(row[0]))), dbg=self.dbg)
                    else:
                        if isinstance(self.insert_query.columns, (dict, sbc.co.OrderedDict)):
                            base = self.insert_query.append_values_dict(row)
                        else:
                            base = self.insert_query.append_values_naive(row)

                        insert = "\n" if not_triggered else ", \n"
                        not_triggered = False

                        if print_dbg:
                            dbc.print_helper((str(i) + " " + base), dbg=self.dbg)
                        self.insert_query.append_query_element(base, append=insert)

            if not not_triggered:
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

            self.insert_query.append_names(self.options["index_name"], append="")
            # TODO: start here
            use_dict_ind = (len(self.options['columns']) > 4)
            self.insert_query.append_insert_names(use_dict=use_dict_ind)

            self.insert_query.print_q_str("db_vertical init", dbg=self.dbg)
            vals = []

            for row in df.iterrows():
                dt_val = row[0] if isinstance(row[0], str) else sbc.convert_timestamp(row[0])

                if np.isnan(row[1][0]):
                    dbc.print_helper(("Excluding: " + dt_val), dbg=self.dbg)
                else:
                    if len(self.options['columns']) == 4:
                        res = (dt_val, df.columns[0], str(row[1][0]), self.options["source"])
                    elif len(self.options['columns']) > 4 and "keys" in self.options.keys():
                        res = self.options['columns'].copy()
                        res[self.options['keys']['date']] = dt_val
                        res[self.options['keys']['id']] = df.columns[0]
                        res[self.options['keys']['value']] = str(row[1][0])
                        # print("HERE -- vertical insert")
                        # print(res.values())
                    else:
                        if self.dbg:
                            print("Warning -- fails insert criteria")

                    vals.append(res)

            if self.mysql_conn is not None:
                success = self.mysql_conn.insert_mutiple_rows(
                    self.insert_query.get_query(), vals)
                dbc.print_helper(("SQL: db_vertical_insert " + str(success)), dbg=self.dbg)
            else:
                dbc.print_helper(("SQL " + self.insert_query.get_query()), dbg=self.dbg)
                print(vals)

    def calc_start_date(self, start_date):
        """ Calculates Start Date (as max date + 1) """
        date = dbc.dt.datetime.now()

        if dbc.dt.datetime.strptime(start_date, "%Y-%m-%d") > date:
            sql = "SELECT * FROM " + self.options["table"] + ";"
            if self.mysql_conn is not None:
                res = self.mysql_conn.query(sql)
                if res:
                    date_final = res[0]['index_date'] + dbc.dt.timedelta(days=1)
                    date_final = dbc.dt.datetime.strftime(date_final, "%Y-%m-%d")
                else:
                    raise ValueError("Empty Result -- calc_start_date")
            else:
                raise ValueError("Mysql Connection must be valid")
        else:
            date_final = start_date
        return date_final

    def execute_info_query(self):
        ''' executes query used to calculate start and end dates '''
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

        return result, determine_periodicity

    def calc_most_recent_date(self, current_max=None):
        ''' Given current max date as string (in %Y-%m-%d format) & current max query determine max
            writeable date
        '''
        ret_value = None

        if self.current_view_query:
            result, determine_periodicity = self.execute_info_query()

            if result and current_max is None:
                dteq = self.extract_db_result_value(result)
                ret_value = bu.dt.datetime.strftime(dteq, "%Y-%m-%d")

            elif result and current_max is not None:
                dte = bu.dt.datetime.strptime(current_max, "%Y-%m-%d")
                dteq = self.extract_db_result_value(result,
                                                    position=self.current_view_query.vars)
                if dteq and isinstance(dteq, bu.dt.date):
                    dteq = bu.dt.datetime(dteq.year, dteq.month, dteq.day)

                if dteq and determine_periodicity:
                    statement_frequency = self.extract_db_result_value(
                        result, (len(result[0])-1))
                    one_day = bu.calc_single_period_advance(
                        dteq.month, dteq.year, statement_frequency)
                    # print("Freq: %s %s" % (statement_frequency, one_day))
                else:
                    one_day = bu.dt.timedelta(days=1)

                if dteq:
                    ret_value = (dteq + one_day if dteq >= dte else dte)
                else:
                    ret_value = dte
            else:
                ret_value = current_max
        else:
            ret_value = current_max

        return ret_value

    def calc_max_date(self):
        ''' calculates maximum date based on view / sp '''
        result, _ = self.execute_info_query()
        ret_value = None

        if result:
            ret_value = str(self.extract_db_result_value(result))
        return ret_value

    def extract_db_result_value(self, result, position=None):
        """ Simple method for extracting results from query / SP results based on position or
            Index_name
        """
        # print(type(result), type(result[0]))
        if isinstance(result[0], dict) and position is not None and isinstance(position, str):
            value = result[0][position]
        if isinstance(result[0], tuple) and position is not None and isinstance(position, int):
            value = result[0][position]
        elif isinstance(result[0], dict) and position is not None and\
                isinstance(position, list) and isinstance(position[0], str):
            value = None
            if result[0][position[0]]:
                value = result[0][position[0]]

            for loc in np.arange(1, len(position)):
                if value and result[0][position[loc]]:
                    value = min(value, result[0][position[loc]])
                elif not value and result[0][position[loc]]:
                    value = result[0][position[loc]]

            if isinstance(value, bu.dt.date):
                value = bu.dt.datetime(value.year, value.month, value.day, 0, 0)
        elif isinstance(result[0], tuple) and "location" in self.options["current_view"]:
            value = result[0][self.options['current_view']['location']]
        else:
            value = result[0][self.options['index_name']]

        return value
