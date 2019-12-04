import re
from .fourd_lib import FourD, FOURD_DATA_TYPES


apilevel = " 2.0 "
threadsafety = 0 
paramstyle = "pyformat"

#PERCENT_PATTERN = re.compile(r'%\(([^\)]+)\)s')
PERCENT_PATTERN = re.compile(r'%\((\w+)\)s')
COLON_PATTERN = re.compile(r':(\w+)')
FORMAT_PATTERN = re.compile(r'%[A-Za-z]')

class Warning(Exception):
    pass

class Error(Exception):
    pass

class InterfaceError(Error):
    pass

class DatabaseError(Error):
    pass

class DataError(DatabaseError):
    pass

class OperationalError(DatabaseError):
    pass

class IntegrityError(DatabaseError):
    pass

class InternalError(DatabaseError):
    pass

class ProgrammingError(DatabaseError):
    pass

class NotSupportedError(DatabaseError):
    pass


class FourD_cursor(object):
    arraysize = 1
    pagesize = 100

    @property
    def __result_type(self):
        return self.result.result_type if self.result else None

    @property
    def rownumber(self):
        return self.result.row_number if self.result else None

    @property
    def description(self):
        return self._description

    @property
    def rowcount(self):
        """"""
        return self.result.row_count if self.result else None

    #----------------------------------------------------------------------
    def setinputsizes(self):
        """"""
        pass

    #----------------------------------------------------------------------
    def setoutputsize(self):
        """"""
        pass

    #----------------------------------------------------------------------
    def __init__(self, connection, fourdconn):
        self.result = None
        self._prepared = False
        self._closed = False
        self.fourdconn = fourdconn
        self.connection = connection
        self._description = None

    def close(self):
        if self.result is not None:
            self.result = None
        self._closed = True
        self._description = None

    def replace_nth(self, source, search, replace, n):
        """Find the Nth occurance of a string, and replace it with another."""
        i = -1
        for _ in range(n):
            i = source.find(search, i+len(search))
            if i == -1:
                return source  #return an unmodified string if there are not n occurances of value

        isinstance(source, str)
        result = "{}{}{}".format(source[:i],replace,source[i+len(search):])
        return result


    def _check_connection(self):
        if self.connection.connected == False:
            raise InternalError("Not connected")
        if self._closed:
            raise InterfaceError("Cursor closed")

    def _describe(self):
        if not self.result or self.result.is_update_count:
            return
        def col_description(col):
            return (col.name, col.pytype, None, None, None, None, None)
        self._description = [col_description(c) for c in self.result.columns]

    def execute(self, query, params=None, describe=True):
        params = params or []
        self._check_connection()
        query.replace('?', chr(1))
        if isinstance(params, dict):
            _params = []
            for pattern in (PERCENT_PATTERN, COLON_PATTERN):
                for key in re.findall(pattern, query):
                    _params.append(params[key])
                query = re.sub(pattern, '?', query)
            params = _params
        query = re.sub(FORMAT_PATTERN, '?', query)
        query.replace('%%', '%')
        
        while True:
            foundtuple = False
            for idx, param in enumerate(params):
                if type(param) == list or type(param) == tuple:
                    foundtuple = True
                    paramlen = len(param)
                    query = self.replace_nth(query, "?",
                                             "({})".format(",".join("?"*paramlen)),
                                             idx+1)  #need 1 based count

                    params = tuple(params[:idx]) + tuple(param) + tuple(params[idx+1:])
                    break  #only handle one tuple at a time, otherwise the idx parameter is off.

            if not foundtuple:
                break
        query.replace(chr(1), '?')
        if not self.connection.in_transaction:
            self.connection._start_transaction()

        if not self._prepared:
            if self.result is not None:
                self.result = None
            self.fourdconn.prepare_statement(query, statement_params=params)
        self.result = self.fourdconn.execute_statement(query, 
                        statement_params=params, 
                        first_page_size= self.pagesize)
        if describe:
            self._describe()

        
    def executemany(self, query, params):
        for execution_param in params:
            self.execute(query, execution_param, describe=False)
            self._prepared = self._prepared or True
        self._describe()
        self.result = None
        self._prepared = False

    def check_fetch(self):
        self._check_connection()
        if not self.__result_type:
            raise DataError("No rows to fetch")

    def fetchone(self):
        self.check_fetch()
        if self.rowcount == 0 or self.result.is_update_count:
            return None
        return self.result.read_row()

    def fetchmany(self, size=arraysize):
        self.check_fetch()
        if self.rowcount == 0 or self.result.is_update_count:
            return []
        result = []
        for i in range(size):
            row = self.fetchone()
            if row is None:
                break
            result.append(row)
        return result

    def fetchall(self):
        self.check_fetch()
        return list(self.result.rows())
        

    def __next__(self):
        result = self.fetchone()
        if result is None:
            raise StopIteration
        return result

    
    def __iter__(self):
        return self

    
    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex_val, tb):
        pass

class FourD_connection:
    
    in_transaction = False

    def __init__(self, host=None, user=None, password=None, 
            database=None, port=None, cursor_factory=None):
        self.cursor_factory = cursor_factory or FourD_cursor
        self.cursors = []
        self.fourdconn = FourD(host=host, user=user, password=password, database=database,
                port=port)
        self.fourdconn.connect()
        self.connected = True
        self.manager_cursor = self.cursor()


    def _start_transaction(self):
        if self.in_transaction:
            return;  
        self.in_transaction = True
        self.manager_cursor.execute("START TRANSACTION;")

    def close(self):
        if self.in_transaction:
            self.manager_cursor.execute("ROLLBACK;")
        if self.connected:
            self.fourdconn.close()
        self.connected = False

    def commit(self):
        if self.in_transaction:
            self.manager_cursor.execute("COMMIT;")
        self.in_transaction = False

    def rollback(self):
        if self.in_transaction:
            self.manager_cursor.execute("ROLLBACK;")
        self.in_transaction = False

    def cursor(self):
        cursor = self.cursor_factory(self, self.fourdconn)
        self.cursors.append(cursor)
        return cursor

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex_val, tb):
        if ex_type is not None:
            if self.in_transaction:
                self.rollback()
                return False
        else:
            if self.in_transaction:
                self.commit()



def connect(dsn=None, host=None, port=None, user=None, password=None, 
    database=None, cursor_factory=None):
    connect_kw = {'cursor_factory':cursor_factory}
    dsn_args = {}
    if dsn is not None:
        dsn_args.update(dict(s.split("=") for s in dsn.split(';')))
    lc = locals()
    for key in ('host','port', 'user', 'password', 'database'):
        connect_kw[key] = lc.get(key) or dsn_args.get(key) or ""
    connect_kw['port'] = connect_kw['port'] or 19812
    connection = FourD_connection(**connect_kw)
    return connection

    


