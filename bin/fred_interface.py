#!/usr/bin/python3
""" Basic Interface to FRED"""
import collections as co
import argparse
import json
import os
import sys
import zipfile
import pandas as pd
from fredapi import Fred
import debug_control as dbc
import backup_utility as bu
import base_interest_rates_interface as rates_dbi



class fred_interface():
    ''' class extension of the fredapi '''
    def __init__(self, options, dbg=False):
        """ Main function that extracts key data points from FRED (St. Louis Federal Reserve)

            =====================================================
            options:  dictionary
            --api_key:  element used to authenticate w/ FRED
            -- items:   element that is dictionary (keys used as Fred requests
                    & values used to replace names  if different from ''
            -- start_date:  first value to request
            -- end_date:    max value to request
        """
        self.df = None
        if options and isinstance(options, dict):
            self.options = options.copy()
        else:
            raise ValueError("Must specify valid options specification")

        self.dbg = dbg

        if "api_key" not in self.options.keys() and "file" in self.options.keys():
            self.load()

        elif "api_key" in self.options.keys():
            self.extract()
        else:
            raise ValueError("Faulty JSON specification")

        if self.df is not None and isinstance(self.df, pd.DataFrame):
            self.clean_names()

        if self.dbg:
            self.__repr__()

    def extract(self):
        ''' extracts data directly apply FRED API '''
        df = {}
        fred = Fred(api_key=self.options["api_key"])
        for item in self.options["items"].keys():
            try:
                if self.options["end_date"]:
                    df[item] = fred.get_series(item, observation_start=self.options["start_date"],
                                               observation_end=self.options["end_date"])
                else:
                    df[item] = fred.get_series(item, observation_start=self.options["start_date"])
            except ValueError as v:
                print("%s : %s " % (item, v))
                continue

        self.df = pd.DataFrame(df)

    def load(self):
        ''' loads FRED data from excel or zip of excel file '''
        if self.options['file']['filename'].lower().endswith("zip"):
            zfile = zipfile.ZipFile(self.options['file']['filename'])

            base_dir = self.options['file']['filename'].split(os.sep)
            ffile = base_dir.pop(len(base_dir)-1)
            base_dir = os.sep.join(base_dir)

            afile = self.options['file']['filename'].replace(".zip", "").replace("_x", ".x")
            ffile = ffile.replace(".zip", "").replace("_x", ".x")
            zfile.extract(ffile, path=base_dir)

            if os.path.exists(afile):
                df = pd.read_excel(afile, sheet_name=self.options['file']['sheet'],
                                   index_col=self.options['file']['col'])

                if 'start_date' in self.options.keys():
                    self.df = df[df.index >= self.options['start_date']].copy()
                else:
                    self.df = df.copy()
                os.remove(afile)
            else:
                raise ValueError("No File Found %s" % (afile))

    def clean_names(self):
        ''' Updates df names '''
        print_dbg = dbc.test_dbg(self.dbg)

        cnts_dict = co.Counter(self.options["items"].values())
        if print_dbg:
            print(cnts_dict)

        if cnts_dict[''] > 0 and cnts_dict[''] < len(self.options["items"]):
            for key, val in self.options["items"].items():
                if val != '':
                    self.df.rename(columns={key: val})
        elif '' not in cnts_dict.keys():
            print("HERE1")
            self.df.rename(index=str, columns=self.options['items'])
            # df.rename(index=str, columns=self.options["items"])
        else:
            print("HERE")

    def __repr__(self):
        ''' base print method '''
        print(self.df.info())
        print(self.df.describe())
        print(self.df.head())
        print(self.df.tail())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Basic CLI interface to FRED")
    parser.add_argument("-a", "--api_key", default="", type=str)
    parser.add_argument("-d", "--dryrun", default=0, type=int)
    parser.add_argument("-e", "--end_date", default=None, type=str)
    parser.add_argument("-f", "--debug_file", type=str)
    parser.add_argument("-i", "--db_host_ip", type=str)

    df_class = None
    parser.add_argument(
        "-l",
        "--items",
        default="DGS1MO,DGS3MO,DGS6MO,DGS1,DGS2,DGS3,DGS5,DGS7,DGS10,DGS20,DGS30",
        type=str, help="Interest Rate indeces to load (conforms to FRED names)"
    )
    parser.add_argument("-o", "--options", default=None, type=str)
    parser.add_argument("-p", "--password", default="", type=str)

    parser.add_argument(
        "-s", "--start_date", default="2001-08-01", type=str
    )
    parser.add_argument("-t", "--table", default="Investing.daily_rate_treasury_data",
                        help="Table where extracted rates are stored")

    parser.add_argument("-u", "--user", default="spennington", type=str)
    parser.add_argument("-v", "--verbose", default=0, type=int)

    args = parser.parse_args()
    args_dict = vars(args)

    dryrun = args.dryrun > 0

    if "options" in args_dict.keys() and args_dict["options"] is not None:
        # Must include host_ip, start_date, items & table
        with open(args_dict["options"], "r") as fp:
            options = json.load(fp)
        fp.close()

        if "items" in options.keys() and isinstance(options["items"], list):
            items = options["items"].copy() # tunr list into OrderedDict

            if "items" in options.keys():
                options.pop("items", "")

            options["items"] = {}
            for itm in items:
                options["items"][itm] = ''

        elif "items" in options.keys() and isinstance(options["items"], dict):
            dict_items = options["items"].copy()

            if "items" in options.keys():
                options.pop("items", "")
            options["items"] = {}

            for key, val in dict_items.items():
                options["items"][key] = val

        else:
            raise ValueError("Faulty Index list")

        if options["end_date"] == "":
            options["end_date"] = None

        if "table" not in options.keys() and not dryrun:
            raise ValueError("Results Table must be specified")

        if args.verbose > 0:
            options["verbose"] = args.verbose
            dbg = True
        else:
            dbg = True
            if "verbose" not in options.keys() and "debug_file" not in options.keys():
                dbg = False
                options["verbose"] = 0
            elif "debug_file" in options.keys():
                options["debug_file"] = bu.append_date_file(options["debug_file"])

        if not dryrun:
            db_interface = rates_dbi.base_rates_db_interface(options, dryrun)
            options["start_date"] = db_interface.calc_start_date(options['start_date'])
        else:
            db_interface = None

        if "append" in options and options["append"] > 0 and "current_view" in options:
            if db_interface:
                options["start_date"] = db_interface.calc_most_recent_date(options["start_date"])
            dbc.print_helper(("Updated start date " + str(options["start_date"])), dbg=dbg)


    elif "db_host_ip" in args_dict.keys() and "items" in args_dict.keys() and\
            "table" in args_dict.keys():
        options = args_dict.copy()
        if "items" in options.keys():
            options.pop("items", "")

        items = args.items.split(',')
        options["items"] = {}
        for itm in items:
            options["items"][itm] = ''


    else:
        raise ValueError("Faulty Configuration")

    try:
        if db_interface:
            print_dbg = db_interface.print_dbg
            dbg = db_interface.dbg
        else:
            dbg, print_dbg = bu.calc_debug_levels(options)

        if "api_key" in options.keys() or "file" in options.keys():
            if db_interface and isinstance(db_interface.dbg, dbc.debug_control):
                df_class = fred_interface(options, dbg=db_interface.dbg)
            else:
                df_class = fred_interface(options, dbg=dbg)

            if df_class:
                df = df_class.df.copy()
            else:
                raise ValueError("Failed to load data frame")
        else:
            raise ValueError("Faulty Configurion: missing api_key")

        if df is not None and df.shape[0] > 0 and df.shape[1] > 0:

            if db_interface and isinstance(db_interface.dbg, dbc.debug_control):
                dbc.error_helper("BD interface exists", dbg=db_interface.dbg)
            elif not db_interface and not dryrun:
                db_interface = rates_dbi.base_rates_db_interface(options, dryrun)

            if not dryrun and db_interface.mysql_conn is not None:
                if df.shape[1] > 1:
                    db_interface.construct_db_insert(df)
                elif df.shape[1] == 1:
                    db_interface.db_vertical_insert(df)
                else:
                    dbc.print_helper("Warning: NO results written", dbg=db_interface.dbg)
            else:
                if dryrun:
                    dbc.print_helper("Warning dryrun is set", dbg=dbg)
                else:
                    raise ValueError("mysql connection was not correctly configured")
        else:
            dbc.print_helper("Faulty dataset", dbg=dbg)

    except ValueError as inst:
        if db_interface:
            dbc.error_helper(("Value Error: " + str(inst)), None, dbg=db_interface.dbg)
        else:
            dbc.error_helper(("Value Error: " + str(inst)), None, dbg=dbg)
    else:
        if db_interface:
            dbc.error_helper(("Failure" + str(sys.exc_info()[0])), None, dbg=db_interface.dbg)
        else:
            dbc.error_helper(("Failure" + str(sys.exc_info()[0])), None, dbg=dbg)
    finally:
        if isinstance(dbg, dbc.debug_control):
            dbg.close()

        if df_class:
            del df_class

        if db_interface:
            del db_interface
