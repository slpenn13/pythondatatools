#!/usr/bin/python3
"""EXTENDED  Interface to FRED"""
# import sys
import datetime as dt
import pandas as pd
import numpy as np
import debug_control as dbc
import base_interest_rates_interface as biri


class rates_db_interface_extended(biri.base_rates_db_interface):
    """ Base class too manage construction and insertion of Rate data """
    def __init__(self, options, dryrun):
        super().__init__(options, dryrun)

        self.sql_update = biri.sbc.sql_query_base(self.options, q_str="UPDATE")
        if self.print_dbg:
            print("Completed Initialization")

    def __del__(self):
        if self.mysql_conn:
            del self.mysql_conn

        if isinstance(self.dbg, dbc.debug_control):
            self.dbg.close()

    def construct_db_insert_update(self, df):
        """ Constructs SQL statement from either data frame or dict(ionary)"""
        df_new = None
        try:
            build_status = -1
            current_df = self.mysql_conn.query(self.options['current_view']['query'])
            if isinstance(current_df, list) and current_df:
                current_df = pd.DataFrame(current_df)
                current_df.index = current_df[self.options['index_name']]

            if isinstance(df, pd.DataFrame) and df.shape[0] >= 1:
                max_date = current_df.iloc[0][self.options['current_view']['max_date']]
                if max_date is None:
                    if self.print_dbg:
                        print("Warning max_date is none")

                    max_date = max(current_df.index_date)

                if self.print_dbg:
                    print("Max Date: %s" % (max_date))

                new_ind = np.logical_and(df.index.year == max_date.year,
                                         df.index.month > max_date.month)
                new_ind = np.logical_or(df.index.year > max_date.year, new_ind)

                new_ind2 = np.logical_and(df.index.year == max_date.year,
                                          df.index.month == max_date.month)

                new_ind2 = np.logical_and(new_ind2, df.index.day > max_date.day)

                new_ind = np.logical_or(new_ind, new_ind2)

                old_ind2 = np.logical_and(df.index.year == max_date.year,
                                          df.index.month < max_date.month)
                old_ind2 = np.logical_or(old_ind2, df.index.year < max_date.year)

                old_ind3 = np.logical_and(df.index.year == max_date.year,
                                          df.index.month == max_date.month)
                old_ind3 = np.logical_and(old_ind3, df.index.day <= max_date.day)

                old_ind2 = np.logical_or(old_ind2, old_ind3)

                old_ind = np.logical_and(df.index > self.options['start_date'],
                                         old_ind2)

                df_new = df[new_ind]
                df_old = pd.merge(df[old_ind], current_df, how='left', left_index=True,
                                  right_index=True)
                if self.print_dbg:
                    print("here")
                    print(df_old.shape, df_new.shape, len(old_ind2), old_ind2.sum(),
                          old_ind.sum())

                if self.mysql_conn is not None:
                    build_status = self.db_dataframe_update(df_old)
                    if self.print_dbg:
                        print("Update Status %d" % (build_status))
                    self.construct_db_insert(df_new)
            elif isinstance(df, dict):
                dbc.error_helper("Failure", None, None, dbg=self.dbg)
                raise ValueError("Error --dict no supported -- (construct_db_insert_update)")
            else:
                dbc.error_helper("Failure", None, None, dbg=self.dbg)
                raise ValueError("Error (construct_db_insert_update)")
        except ValueError as v:
            print("Failed Update {}".format(v))
        except biri.dbsql.mysqldb.Error as err:
            print("Failed Update: {}".format(err))

        return df_new

    def db_dataframe_update(self, df):
        """ Constructs SQL UPDATE from DataFRame, assumes current and new merged into
            single table
        """
        build_status = -1
        if isinstance(df, pd.DataFrame) and not df.empty:
            build_status = 0

            for item in df.iterrows():
                for key, value in self.options['items'].items():
                    if key in df.columns.to_list() and value in df.columns.to_list():
                        try:
                            if not np.isnan(item[1][key]) and np.isnan(item[1][value]):
                                write_dict = {"result": item[1][key],
                                              "date": biri.bu.dt.date(item[0].year, item[0].month,
                                                                      item[0].day)}

                                q_str = self.sql_update.q_str.replace("%(field)s", value)

                                if self.print_dbg:
                                    print("%s -- %s %s %f %f \n %s" % (
                                        item[0], key, value, item[1][key], item[1][value],
                                        q_str))

                                build_status += self.mysql_conn.update(
                                    q_str, write_dict)

                        except ValueError as v:
                            print("Failed Update {}".format(v))
                            continue
                        except biri.dbsql.mysqldb.Error as err:
                            print("Failed Update: {}".format(err))
                            continue
                    else:
                        if self.print_dbg:
                            print("Not found %s %s" % (key, value))

        return build_status

    def db_dict_update(self, dict_res):
        """ Constructs SQL UPDATE from DataFRame, assumes current and new merged into
            single table
        """
        build_status = -1
        if dict_res and isinstance(dict_res, dict):
            build_status = 0
            q_str = self.sql_update.q_str

            for key, row in dict_res.items():
                try:
                    write_dict = row.copy()
                    if isinstance(key, dt.datetime):
                        write_dict['index'] = dt.datetime.strftime(key, "%Y-%m-%d")
                    else:
                        write_dict['index'] = key

                    if self.print_dbg:
                        print("%s -- %f %f %f %f %f \n %s" % (
                            key, write_dict['CAD'], write_dict['EUR'], write_dict['GBP'],
                            write_dict['JPY'], write_dict['CNY'], q_str))

                    build_status += self.mysql_conn.update(
                        q_str, write_dict)

                except ValueError as v:
                    print("Failed Update {}".format(v))
                    continue
                except biri.dbsql.mysqldb.Error as err:
                    print("Failed Update: {}".format(err))
                    continue
                #else:
                #    if self.print_dbg:
                #        print("Not found %s" % (key))

        return build_status
