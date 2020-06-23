# -*- coding: utf-8 -*-
#!/usr/bin/python3
''' Exhange Rate loader '''
import argparse
import json
import os
import requests

def load_exchange_rates(options, dbg=False):
    ''' excahge rate etractor '''

    req_str = build_req_str(options, dbg=dbg)
    req = "/".join([options['url'], req_str])
    result = requests.get(req)

    if result.ok:
        if 'save_file' in options.keys():
            with open(options['save_file'], 'wb') as fp:
                for chunk in result.iter_content(10000):
                    fp.write(chunk) #ToDo: json parsing
            fp.close()

        result_dict = json.loads(result.content)

    else:
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
            req_str = '?'.join([req_str, 'base',
                                options['url_control']['base'].upper()])
    else:
        req_str = "latest"

    if dbg:
        print(req_str)

    return req_str


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Initial exchange rate tool"
    )

    parser.add_argument("-o", "--options", default=None, type=str)
    parser.add_argument("-v", "--verbose", default=0, type=int)

    args = parser.parse_args()
    args_dict = vars(args)

    dbg = bool('verbose' in args_dict.keys() and int(args_dict['verbose']) > 0)

    if "options" in args_dict.keys():
        if os.path.exists(args_dict['options']):
            with open(args_dict['options'], 'r') as fp:
                options = json.load(fp)
            fp.close()
        else:
            raise ValueError("Options File Does not exist!!!")
    else:
        raise ValueError("Options Dictionary Provided")


    result_dict = load_exchange_rates(options, dbg=dbg)
