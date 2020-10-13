# -*- coding: utf-8 -*-
#!/usr/bin/python3
''' Exhange Rate loader '''
import argparse
import json
import os
import requests
import backup_utility as bu
import debug_control as dbc
import interest_rates_interface_extended as rates_dbi


def load_exchange_rates(options, dbg=False):
    ''' excahge rate etractor '''

    req_str = build_req_str(options, dbg=dbg)
    req = "/".join([options['url_control']['url'], req_str])
    result = requests.get(req)

    if result.ok:
        if 'save_file' in options.keys():
            with open(options['save_file'], 'wb') as fp:
                for chunk in result.iter_content(10000):
                    fp.write(chunk)
            fp.close()

        result_dict = json.loads(result.content)

    else:
        if dbg:
            print(result.reason)
        raise ValueError("Failed data extraction ")

    return result_dict

def build_req_str(options, dbg=False):
    ''' constructs request strings used in REST request '''
    req_str = ''

    if "url_control" in options.keys():
        if "start_date" in options['url_control'] and "end_date" in options['url_control']:
            req_str = "history?start_at"
            mid = options['url_control']['start_date'] + '&end_at'
            req_str = "=".join([req_str, mid, options['url_control']['end_date']])

            # ToDo: improve date parsing
        elif 'date' in options['url_control']:
            req_str = str(options['url_control']['date'])
        else:
            if dbg:
                print("Warning: exchage rate loader defaulting to latest")

            req_str = 'latest'

        if 'symbols' in options['url_control'] and options['url_control']['symbols'] and\
                isinstance(options['url_control']['symbols'], list):
            init = ",".join(options['url_control']['symbols']).upper()
            req_str = "".join([req_str, "&symbols=", init])

        if 'base' in options['url_control'] and options['url_control']['base'] and\
                options['url_control']['base'].upper() != 'EUR':
            base = "=".join(['base', options['url_control']['base'].upper()])
            req_str = '&'.join([req_str, base])
    else:
        req_str = "latest"

    if dbg:
        print(req_str)

    return req_str


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Initial exchange rate tool"
    )

    parser.add_argument("-d", "--dryrun", default=0, type=int)
    parser.add_argument("-o", "--options", default=None, type=str)
    parser.add_argument("-v", "--verbose", default=0, type=int)

    args = parser.parse_args()
    args_dict = vars(args)

    dryrun = args.dryrun > 0
    dbg = bool('verbose' in args_dict.keys() and int(args_dict['verbose']) > 0)

    if "options" in args_dict.keys():
        if os.path.exists(args_dict['options']):
            with open(args_dict['options'], 'r') as fp:
                options = json.load(fp)
            fp.close()
        else:
            raise ValueError("Options File Does not exist!!!")

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

        if "items" in options.keys() and isinstance(options["items"], dict):
            dict_items = options["items"].copy()

            if "items" in options.keys():
                options.pop("items", "")
            options["items"] = {}
            symbols_missing = False
            if "symbols" not in options["url_control"].keys() or(\
                    "symbols" in options['url_control'].keys() and not
                    options['url_control']['symbols']):
                options["url_control"]["symbols"] = []
                symbols_missing = True

            for key, val in dict_items.items():
                options["items"][key] = val
                if symbols_missing:
                    options['url_control']['symbols'].append(key)
        else:
            raise ValueError("Faulty Index list")

        if not dryrun:
            db_interface = rates_dbi.rates_db_interface_extended(options, dryrun)
            options["url_control"]["start_date"] = db_interface.calc_start_date(
                options["url_control"]["start_date"])
        else:
            db_interface = None

        if "append" in options and options["append"] > 0 and "current_view" in options:
            db_interface.options["url_control"]["start_date"] =\
                    db_interface.calc_most_recent_date(options["url_control"]["start_date"])

            db_interface.options["url_control"]["start_date"] = bu.dt.datetime.strftime(
                db_interface.options["url_control"]["start_date"], "%Y-%m-%d")

            dbc.print_helper(("Updated start date " +
                              db_interface.options["url_control"]["start_date"]),
                             dbg=dbg)

            max_date = db_interface.calc_max_date()
            if max_date:
                db_interface.options['url_control']['end_date'] = str(max_date)

    else:
        raise ValueError("Options Dictionary Provided")

    if db_interface:
        result_dict = load_exchange_rates(db_interface.options, dbg=dbg)
    else:
        result_dict = load_exchange_rates(options, dbg=dbg)

    if db_interface and result_dict and isinstance(result_dict, dict) and\
            'rates' in result_dict.keys() and isinstance(result_dict['rates'], dict):
        bld = db_interface.db_dict_update(result_dict['rates'])

        del db_interface
