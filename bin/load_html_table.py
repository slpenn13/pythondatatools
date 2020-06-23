# -*- coding: utf-8 -*-
#!/usr/bin/python3
''' Exhange Rate loader '''
import argparse
import json
import os
import bs4
import pandas as pd
import requests

def parse_table(options, dbg=False):
    ''' excahge rate etractor '''
    if not isinstance(options, dict):
        raise ValueError("(parse table) faulty type -- expecting dict")
    result = None
    result_dict = None

    if 'html' in options.keys() and isinstance(options['html'], str) and\
            os.path.exists(options['html']):
        with  open(options['html'], 'r') as fp:
            soup = bs4.BeautifulSoup(fp.read(), 'html.parser')
        fp.close()

    elif  'url' in options.keys():
        result = requests.get(options['url'])

        if not result.ok:
            raise ValueError("Faulty URL read")

        soup = bs4.BeautifulSoup(result.text, "html.parser")

    else:
        raise ValueError("NO reference HTML found")

    if options['type'] != 0:
        if 'save_file' in options.keys():
            with open(options['save_file'], 'wb') as fp:
                for chunk in result.iter_content(10000):
                    fp.write(chunk)
            fp.close()

        result_dict = parse_bs4_currency(soup, options, dbg=dbg)
    else:
        if isinstance(result, requests.models.Response):
            result_dict = parse_pandas_df(result.content, options, dbg=dbg)
        else:
            result_dict = parse_pandas_df(options['html'], options, dbg=dbg)
        if isinstance(result_dict, pd.DataFrame) and all(result_dict.shape) > 0 and\
                'save_file' in options.keys():
            result_dict.to_csv(options['save_file'], header=True, index=True, index_label='index')

    return result_dict

def parse_bs4_currency(soup, options, dbg=False):
    ''' constructs dictoinary of currencies with correspondinf states & counts'''
    result_dict = {}
    elems = soup.select(options['select']['initial'])

    for cnt, itm in zip(range(0, len(elems), 1), elems):
        cols = itm.select(options['select']['column'])
        if cols and len(cols) > 1:
            currency = cols[options['dict']['key']].getText()
            if currency in result_dict.keys():
                result_dict[currency]['countries'].append(\
                    cols[options['dict']['country']].getText())
                result_dict[currency]['count'] += 1
            else:
                if dbg:
                    print("Adding new currency %s " % (currency))
                result_dict[currency] = {'countries': [], 'count': 1}
                result_dict[currency]['countries'].append(\
                        cols[options['dict']['country']].getText())
        else:
            print("Exclding item: %d  %s" % (cnt, itm.getText()))


    return result_dict

def parse_pandas_df(content, options, dbg=False):
    ''' constructs data frame of for table extracted from html '''
    tbls = pd.read_html(content)

    pos = 0
    if 'select' in options.keys() and 'table_loc' in options['select'].keys():
        pos = int(options['select']['table_loc'])

    if tbls and len(tbls) > pos:
        rslt_df = tbls[pos]
    else:
        raise ValueError("No table extracted!!!")

    if 'select' in options.keys() and (\
            'clean_columns' in options['select'].keys() or\
            'clean_index' in options['select'].keys()):
        rslt_df = parse_header_df(rslt_df, options, dbg=dbg)
        if 'cleanse' in options.keys() and int(options['cleanse']) > 0:
            rslt_df = cleanse_df(rslt_df, options, dbg=dbg)

    return rslt_df

def cleanse_df(df, options, dbg=False):
    ''' rules for excluding rows of DF '''

    ind = (df.iloc[:, options['select']['index_loc']].str.len() > 1)
    if dbg:
        print("Warning (cleanse) -- droppring %d or %d" % (
            (df.shape[0] -ind.sum()), df.shape[0]))

    df = df[ind]

    return df
def parse_header_df(df, options, dbg=False):
    ''' constructs cleaned header columns '''

    if 'clean_columns' in options['select'].keys() and int(options['select']['clean_columns']) > 0:
        col_list = []
        for itm in df.columns:
            col_list.append(parse_text(itm, options))

        if col_list and len(col_list) == len(df.columns):
            df.columns = col_list
        else:
            if dbg:
                print("Warning -- no columns updated")

    if 'clean_index' in options['select'].keys() and int(options['select']['clean_index']) > 0 and\
            'index_loc' in options['select'].keys():
        loc = int(options['select']['index_loc'])
        index_list = []

        for row in df.iterrows():
            index_list.append(parse_text(row[1][loc], options))

        if index_list and len(index_list) == df.shape[0]:
            df.index = index_list
        else:
            if dbg:
                print("Warning -- index elements updated")

    return df

def parse_text(text, options):
    ''' parses key text into common format base [control][clean] in options dict (json)'''

    text_fnl = text
    if options and 'control' in options.keys():
        if 'clean' in options['control'].keys() and\
                options['control']['clean'] and isinstance(options['control']['clean'], dict):
            for key, value in options['control']['clean'].items():
                text_fnl = text_fnl.replace(key, value)

        #if text.find("Cote") >= 0:
        #    print(text_fnl)
        if 'additional' in options['control'].keys() and\
                options['control']['additional'] and\
                isinstance(options['control']['additional'], dict):
            # print("Running Additional")
            for key, value in options['control']['additional'].items():
                text_fnl = text_fnl.replace(key, value)



        #if text.find("Cote") >= 0:
        #    print(text_fnl)
        if 'capitalize' in options['control'] and\
                int(options['control']['capitalize']) > 0:
            text_fnl = text_fnl.upper()
        elif  'capitalize' in options['control'] and\
                int(options['control']['capitalize']) == 0:
            text_fnl = text_fnl.lower()

    return text_fnl

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Initial exchange rate tool"
    )

    parser.add_argument("-o", "--options", default=None, type=str)
    parser.add_argument("-v", "--verbose", default=0, type=int)

    args = parser.parse_args()
    args_dict = vars(args)

    dbg = ('verbose' in args_dict.keys() and int(args_dict['verbose']) > 0)

    if "options" in args_dict.keys():
        if os.path.exists(args_dict['options']):
            with open(args_dict['options'], 'r') as fp:
                options = json.load(fp)
            fp.close()
        else:
            raise ValueError("Options File Does not exist!!!")
    else:
        raise ValueError("Options Dictionary Provided")


    result_dict = parse_table(options, dbg=dbg)
