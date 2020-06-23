""" Class wrapper corresponding to SQL specification """
#!/usr/bin/python3
import collections as co
from enum import Enum, unique
import debug_control as dbc

@unique
class sql_type(Enum):
    """ Enumerations determining type of SQL action """
    SELECT = 1
    STORED_PROCEDURE_RES = 2
    STORED_PROCEDURE_NO_RES = 3
    INSERT = 4


class sql_query_base():
    """ Stores information regarding query """

    def __init__(self, info, table=None, q_str="SELECT"):
        """ sql_query_base cobnstructor: requires table or table or infor["table"] """
        self.columns = None
        if table is not None:
            self.table = table
        elif info is not None and isinstance(info, dict) and\
                "table" in info.keys():
            self.table = info["table"]
        else:
            raise ValueError("Incomplete table specification")

        if info is not None and isinstance(info, dict) and\
                "index_name" in info.keys():
            self.index_name = info["index_name"]
        else:
            raise ValueError("Incomplete index specification")

        if q_str.upper().startswith("INSERT"):
            self.construct_insert(info)

        elif q_str.upper().startswith("SELECT"):
            self.sql_type_ind = sql_type.SELECT
            self.return_result = True
            self.q_str = q_str
        else:
            self.construct_sp(info)

        if "vars" in info.keys() and isinstance(info["vars"], (list, dict)):
            self.vars = info["vars"].copy()
        else:
            self.vars = None

    def construct_insert(self, info):
        """ initializes query in the case of INSERT """
        self.sql_type_ind = sql_type.INSERT
        self.return_result = False
        self.q_str = "generate"

        if  "items" in info.keys() and len(info["items"]) == 1 and\
                "columns" in info.keys() and isinstance(info["columns"], dict):
            self.columns = co.OrderedDict()
            for key, val in info["columns"].items():
                self.columns[key] = val

        elif "items" in info.keys() and isinstance(info["items"], dict) and\
                len(info["items"]) > 1:
            self.columns = co.OrderedDict()
            for key, val in info["items"].items():
                if val != '':
                    self.columns[val] = key
                else:
                    self.columns[key] = key
        elif "items" in info.keys() and isinstance(info["items"], list):
            self.columns = co.OrderedDict()
            for val in info["items"]:
                self.columns[val] = ''

    def construct_sp(self, info):
        """ initializes stored procedure information """
        if "query" in info.keys():
            if info["query"].upper().startswith("CALL"):
                self.q_str = info["query"]
                self.sql_type_ind = (info["q_type_ind"] if "q_type_ind" in info.keys() else
                                     sql_type.STORED_PROCEDURE_NO_RES)

                self.return_result = bool((self.sql_type_ind is sql_type.SELECT or\
                                           self.sql_type_ind is sql_type.STORED_PROCEDURE_RES))

        elif "procedure" in info.keys():
            self.q_str = info["procedure"]

            self.sql_type_ind = (info["q_type_ind"] if "q_type_ind" in info.keys() else
                                 sql_type.STORED_PROCEDURE_NO_RES)

            self.return_result = bool((self.sql_type_ind is sql_type.SELECT or\
                                       self.sql_type_ind is sql_type.STORED_PROCEDURE_RES))

    def get_table(self):
        """ return impacted table """
        return self.table

    def get_index_name(self):
        """ return index element """
        return self.index_name

    def get_query(self):
        """ return current query """
        return self.q_str

    def construct_insert_start(self, include_index=False, dbg=False):
        """ initiates sql_init """
        if self.q_str.upper() != "GENERATE":
            if dbg:
                print("No initial query added")
        else:
            self.q_str = ""

        if isinstance(self.index_name, str):
            if include_index:
                final = "".join([" (", self.index_name, ", "])
            else:
                final = " ("

        elif isinstance(self.index_name, list):
            if include_index:
                init = ", ".join(self.index_name)
                final = "".join([" (", init, ", "])
            else:
                final = " ("
        else:
            raise ValueError("index name is faulty")

        self.q_str = "".join(["INSERT INTO ", self.table, final])

    def append_names(self, excludes=None, append=", "):
        """ appends names + returns chosen columns """

        if self.columns and self.q_str.startswith("INSERT"):
            q_temp = []
            if excludes and isinstance(excludes, str):
                for key in self.columns.keys():
                    if key != excludes:
                        q_temp.append(key)
            elif excludes and isinstance(excludes, list):
                for key in self.columns.keys():
                    if key not in excludes:
                        q_temp.append(key)
            elif excludes is None:
                for key in self.columns.keys():
                    q_temp.append(key)
            else:
                raise ValueError("append_names accepts lists, dict keys and pd.DataFrame indexes")
        else:
            raise ValueError("append_names does not accept NULL init_objections")

        q_temp = ", ".join(q_temp)
        self.q_str = append.join([self.q_str, q_temp])
        self.q_str = "".join([self.q_str, ") VALUES "])

    def append_query_element(self, val, append=", "):
        """ append element to current q_str using user spec'd split """
        self.q_str = append.join([self.q_str, val])

    def clean_query_element(self, search="), \n", replace=");"):
        """ cleans final element if ends with search (default='), \n') """
        if len(search) > 1 and self.q_str.endswith(search):
            ln1 = len(search)
            self.q_str = self.q_str[:-ln1] + replace

    def print_q_str(self, insert_str="", dbg=True):
        """ quick debug output """
        dbc.print_helper((
            "sql_class_base(" + insert_str + "): query string: " + self.q_str), dbg=dbg)


def calculate_current_view(q_view, index_name):
    """ Builds (Calls sql_query_base constructor) sql_query_base based in q_view """
    determine_periodicity = False
    info = {}
    result = None

    if isinstance(q_view, str) and\
        q_view.upper().startswith("SELECT"):
        q_table = calc_table_name(q_view, sql_type.SELECT)
        info["table"] = q_table
        info["index_name"] = index_name

        result = sql_query_base(info, q_str=q_view)

    elif isinstance(q_view, str) and\
            not q_view.startswith("SELECT"):
        info["table"] = q_view
        q_str = " ".join(["SELECT * FROM", q_view, ";"])
        info["index_name"] = index_name

        result = sql_query_base(info, q_str=q_str)

    elif isinstance(q_view, dict) and\
            "query" in q_view.keys() and\
            q_view["query"].upper().startswith("SELECT"):
        q_table = calc_table_name(q_view["query"], sql_type.SELECT)
        info["table"] = q_table
        info["index_name"] = index_name

        if "vars" in q_view.keys():
            info["vars"] = q_view["vars"].copy()

        result = sql_query_base(info, q_str=q_view["query"])


    elif isinstance(q_view, dict) and\
            "procedure" in q_view.keys():
        info["table"] = q_view["procedure"]
        info["procedure"] = q_view["procedure"]
        info["index_name"] = index_name
        info["q_type_ind"] = sql_type.STORED_PROCEDURE_RES

        if "vars" in q_view.keys():
            info["vars"] = q_view["vars"].copy()

        result = sql_query_base(info, q_str="CALL")

        if "location" in q_view.keys():
            determine_periodicity = True
    else:
        print(info)
        raise ValueError("Failed Run: calculate_current_view!!!")

    return result, determine_periodicity


def calc_table_name(q_str, qtype):
    """ Calculates SQL table from query string """
    table = None
    q_split = q_str.split() # Quick split => all white space throws away empties
    if qtype is sql_type.SELECT:
        fnd = False
        for itm in q_split:
            if itm.upper() == "FROM":
                fnd = True
            elif fnd and itm != "":
                table = itm
    elif qtype is sql_type.STORED_PROCEDURE_RES:
        fnd = False
        for itm in q_split:
            if itm.upper() == "CALL":
                fnd = True
            elif fnd and itm != "" and itm != "*":
                table = itm

    return table
