import socket
import base64
from collections import namedtuple, defaultdict, deque
from datetime import datetime, time
import logging
import struct
log = logging.getLogger('fourd')
log.setLevel(logging.DEBUG)

DEFAULT_IMAGE_TYPE="png"

__LOGIN_BASE64__=True
__STATEMENT_BASE64__=True

bCRLF=b'\r\n'
bCR=b'\r'
bLF=b'\n'
bCOLON=b':'

SPACE=' '

FOURD_DATA_TYPES= {
    0:"VK_UNKNOW",
    1:"VK_BOOLEAN",
    3:"VK_WORD",
    4:"VK_LONG",
    5:"VK_LONG8",
    6:"VK_REAL",
    7:"VK_FLOAT",
    8:"VK_TIMESTAMP",
    9:"VK_DURATION",
    10:"VK_STRING",
    18:"VK_BLOB",
    12:"VK_IMAGE",
    21:"VK_BLOB",}

RESULT_SET='Result-Set'
UPDATE_COUNT='Update-Count'
OK='OK'

fourD_str_types = {
    "VK_BOOLEAN":bool,
    "VK_BYTE":str,
    "VK_WORD":int,
    "VK_LONG":int,
    "VK_LONG8":int,
    "VK_REAL":float,
    "VK_FLOAT":float,
    "VK_TIMESTAMP":datetime,
    "VK_TIME":datetime,
    "VK_DURATION":time,
    "VK_STRING":str,
    "VK_BLOB":bytes,
    "VK_IMAGE":bytes,
    "VK_UNKNOW":None
}

fourD_types_str = defaultdict(lambda:"VK_STRING",{
    bool:"VK_BOOLEAN",
    int:"VK_LONG8",
    float:"VK_REAL",
    datetime:"VK_TIMESTAMP",
    #datetime:"VK_TIME",
    time:"VK_DURATION",
    str:"VK_STRING",
    bytes:"VK_BLOB",
    type(None):"VK_UNKNOW"
})

class FourDColumn:
    __slots__=('name', 'dtype','pytype', 'updatable')
    def __init__(self, *args, **kwargs):
        for key in self.__slots__:
            setattr(self,key,kwargs.get(key))

class FourDException(Exception):
    """Standard 4D Exception"""
    caption = """!!Error code {code} (component code {component_code}) : {description}."""
    localizer = None
    
    def __init__(self, description=None, code=None, component_code=None, **kwargs):
        self.description = description
        if isinstance(code,bytes):
            code = code.decode()
        if isinstance(component_code,bytes):
            component_code = component_code.decode()
        if isinstance(description,bytes):
            description = description.decode()
        self.code = code
        self.component_code = component_code
        self.description = description
        
    def __str__(self):
        caption_args = dict(code=self.code, 
            description=self.description, component_code=self.component_code)
        return self.caption.format(**caption_args)

class FourDCommand:
    cmd_id = 0
    cmd_txt = ''
    cmd_suffix = ''
    cmd_params = []
    def __init__(self, *args, **kwargs):
        self.params = []
        for cmd_param in self.cmd_params:
            kw_param_base64_encode = False
            kw_name = cmd_param
            if kw_name.endswith('-BASE64'):
                kw_param_base64_encode = True
                kw_name = cmd_param[:-7]
            kw_name = kw_name.lower().replace('-','_')
            value = kwargs.get(kw_name)
            if value is None:
                continue
            if isinstance(value, bool):
                value = 'Y' if value else 'N'
            if kw_param_base64_encode:
                value = base64.b64encode(value.encode())
            if isinstance(value, bytes):
                value = value.decode()
            self.params.append('{}: {}'.format(cmd_param,value).encode())

    def __bytes__(self):
        return self.bytes()
        

    def bytes(self, include_binary=True):
        out = '{:03} {}'.format(self.cmd_id, self.cmd_txt).encode()+bCRLF
        if self.params:
            out += bCRLF.join(self.params)+bCRLF
        if self.cmd_suffix:
            out += self.cmd_suffix.encode()+bCRLF
        out += bCRLF
        if include_binary and hasattr(self,'binary_data') and self.binary_data:
            out += self.binary_data
        return out

    def __repr__(self):
        return bytes(self.bytes(include_binary=False)).decode()


class FourDLogin(FourDCommand):
    cmd_id = 1
    cmd_txt = 'LOGIN'
    cmd_params = ['USER-NAME-BASE64', 'USER-PASSWORD-BASE64', 
    'REPLY-WITH-BASE64-TEXT', 'PREFERRED-IMAGE-TYPES', 'PROTOCOL-VERSION']
    cmd_suffix = 'PROTOCOL-VERSION: 13.0'

class FourDLoginPlain(FourDCommand):
    cmd_id = 1
    cmd_txt = 'LOGIN'
    cmd_params = ['USER-NAME', 'USER-PASSWORD', 
    'REPLY-WITH-BASE64-TEXT', 'PREFERRED-IMAGE-TYPES']  

class FourDLogout(FourDCommand):
    cmd_id = 4
    cmd_txt = 'LOGOUT'

class FourDQuit(FourDCommand):
    cmd_id = 5
    cmd_txt = 'QUIT'

class FourDBaseStatement(FourDCommand):
    def __init__(self, statement=None, statement_params=None, **kwargs):
        self.binary_data = b''
        parameter_types =self.bind_statement_params(statement_params)
        statement_kwargs=dict(statement=statement)
        statement_kwargs.update(kwargs)
        if parameter_types:
            statement_kwargs['parameter_types'] = ' '.join(parameter_types)
        super().__init__(**statement_kwargs)

    def bind_statement_params(self, statement_params):
        parameter_types = []
        if not statement_params:
            return
        for statement_param in statement_params:
            parameter_type = fourD_types_str.get(type(statement_param))
            serializer = getattr(self, 'serialize_%s'%parameter_type)
            if statement_param is not None:
                parameter_data = b'1'+serializer(statement_param)
            else:
                parameter_data = b'0'
            self.binary_data += parameter_data
            parameter_types.append(parameter_type)
        return parameter_types

    def serialize_VK_BOOLEAN(self, statement_param):
        return struct.pack('<H', statement_param)

    def serialize_VK_LONG8(self, statement_param):
        return struct.pack('<q', statement_param)

    def serialize_VK_REAL(self, statement_param):
        return struct.pack('<d', statement_param)

    def serialize_VK_TIMESTAMP(self, statement_param):
        year = statement_param.year
        month = statement_param.month
        day = statement_param.day
        milliseconds = (statement_param.hour*3600+statement_param.minute*60+statement_param.second)*1000
        return struct.pack('<HBBL',year,month,day,milliseconds)

    serialize_VK_TIME = serialize_VK_TIMESTAMP
        

    def serialize_VK_DURATION(self, statement_param):
        hour = statement_param.hour
        minute = statement_param.minute
        second = statement_param.second
        millisecond = statement_param.millisecond//1000
        millisecond = millisecond + second*1000 + minute*60*1000 + hour*3600*1000
        return struct.pack('<D', millisecond)

    def serialize_VK_STRING(self, statement_param):
        encoded_value=statement_param.encode('UTF-16LE')
        return struct.pack('<l',-len(statement_param))+encoded_value

    def serialize_VK_BLOB(self, statement_param):
        return struct.pack('<l',len(statement_param))+statement_param

    def serialize_VK_UNKNOW(self, statement_param):
        pass


class FourDPrepareStatement(FourDBaseStatement):
    cmd_id = 3
    cmd_txt = 'PREPARE-STATEMENT'
    cmd_params = ['STATEMENT-BASE64','PARAMETER-TYPES']

class FourDPrepareStatementPlain(FourDBaseStatement):
    cmd_id = 3
    cmd_txt = 'PREPARE-STATEMENT'
    cmd_params = ['STATEMENT', 'PARAMETER-TYPES']

class FourDExecuteStatement(FourDBaseStatement):
    cmd_id = 3
    cmd_txt = 'EXECUTE-STATEMENT'
    cmd_params = ['STATEMENT-BASE64','PARAMETER-TYPES','FIRST-PAGE-SIZE',
        'OUTPUT-MODE','FULL-ERROR-STACK']

class FourDExecuteStatementPlain(FourDBaseStatement):
    cmd_id = 3
    cmd_txt = 'EXECUTE-STATEMENT'
    cmd_params = ['STATEMENT','PARAMETER-TYPES','FIRST-PAGE-SIZE',
        'OUTPUT-MODE','FULL-ERROR-STACK']

class FourDFetchStatement(FourDBaseStatement):
    cmd_id = 123
    cmd_txt = 'FETCH-RESULT'
    cmd_params = ['STATEMENT-ID','COMMAND-INDEX','OUTPUT-MODE',
        'FIRST-ROW-INDEX', 'LAST-ROW-INDEX', 'FULL-ERROR-STACK']

class FourDCloseStatement(FourDCommand):
    cmd_id = 0
    cmd_txt = 'CLOSE-STATEMENT'
    cmd_params = ['STATEMENT-ID']


class FourDResponse:
    def __init__(self, connection=None):
        self.connection = connection
        self.socket = connection.socket
        self.headers = {}
        self.read_headers()
        if not self.OK:
            raise self.exception
        self.row_number = None
        if self.is_result_set:
            self.row_count_received = 0
            self.row_number = 0


    def dis__del__(self):
        if self.is_result_set or self.is_update_count:
            if self.connection.connected:
                self.close()

    def close(self):
        if self.statement_id:
            statement_cmd = FourDCloseStatement(statement_id=self.statement_id)
            self.connection._socket_send(statement_cmd)
            header_bytes = self._read_header_bytes()
            status_line, header_lines = self._get_header_lines(header_bytes)
            status_code, statement_code = self._decode_status(status_line)
            self.__statement_id = None

    def _read_header_bytes(self):
        header_bytes = bytearray()
        header_found=False
        data = self._recv(1)
        while data and not header_found:
            header_bytes.extend(data)
            if header_bytes.endswith(b'\r\n\r\n'):
                header_found = True
                break
            data = self._recv(1)
        if not header_found:
            raise Exception("Error: Header-end not found\n")
        return header_bytes

    def _get_header_lines(self, header_bytes):
        header_bytes = header_bytes.strip(2*bCRLF)
        header_bytes = header_bytes.replace(bCRLF,bLF)
        header_lines = header_bytes.split(bLF)
        status_line = header_lines.pop(0)
        return status_line, header_lines
        
    def _decode_status(self, status_line):
        statement_code,_, status_code = status_line.decode().partition(SPACE)
        return status_code, statement_code


    @property
    def result_type(self):
        return self.headers.get('Result-Type')

    def read_headers(self):
        header_bytes = self._read_header_bytes() 
        status_line, header_lines = self._get_header_lines(header_bytes)
        self.status_code, self.statement_code = self._decode_status(status_line)
        for header_line in header_lines:
            key, value = header_line.split(bCOLON)
            key = bytes(key).decode()
            if key.endswith('-Base64'):
                key = key.replace('-Base64','')
                value = base64.b64decode(value)
            value = bytes(value.strip()).decode()
            self.headers[key] = value

    @property
    def columns(self):
        if not hasattr(self,'_columns'):
            self._columns = self._read_columns()
        return self._columns

    @property
    def row_count(self):
        if self.is_result_set:
            return int(self.headers.get('Row-Count',0))
        elif self.is_update_count:
            return self.update_count

    @property
    def update_count(self):
        if not hasattr(self,'_update_count'):
            self._update_count = self._read_update_count()
        return self._update_count

    def _read_update_count(self):
        if self.is_update_count:
            self._recv(1)
            return self.deserialize_VK_LONG8()

    @property
    def is_update_count(self):
        return self.result_type == UPDATE_COUNT


    @property
    def initial_row_count_sent(self):
        return int(self.headers.get('Row-Count-Sent',0))

    @property
    def updatable(self):
        if not hasattr(self, '_updatable'):
            self._updatable = self.columns and any(map(lambda c:c.updatable, self.columns))
        return self._updatable
        
    def _read_columns(self):
        columns = []
        if not self.is_result_set:
            return 
        n_columns = int(self.headers.get('Column-Count', 0))
        column_names = self.headers.get('Column-Aliases', '')
        column_names = column_names.lstrip('[').rstrip(']').split('] [')
        column_types = self.headers.get('Column-Types', '').split()
        column_updatability = self.headers.get('Column-Updateability','').split()
        
        for i in range(n_columns):
            column_name = column_names[i]
            column_type = column_types[i]
            column_updatable = column_updatability[i] == 'Y'
            pytype = fourD_str_types.get(column_type)
            columns.append(FourDColumn(name=column_name, dtype=column_type,
                updatable=column_updatable, pytype=pytype))
        self._row_factory = namedtuple('row', column_names)
        return columns

    def _initialize(self):
        if self.is_result_set:
            self._rows_cache
        elif self.is_update_count:
            self.update_count

    @property
    def _rows_cache(self):
        if not hasattr(self, '_rows_deque'): # cache the initial rows
            self._rows_deque = deque()
            while self.row_count_received<self.initial_row_count_sent:
                self._rows_deque.append(self._read_row())
        return self._rows_deque
            
    def _read_row(self):
        self.row_count_received +=1 
        row_id=None
        if self.updatable:
            status = self._recv(1)
            row_id = self.deserialize_VK_LONG()
        row_values = {}
        value=None
        for column in self.columns:
            status = self._recv(1)
            try:
                status = int(status)
            except:
                raise Exception('Error in reading status byte')
            if status == 0:
                value = None
            elif status == 1:
                value = self._read_value(column)
            elif status == 2:
                error_code =  self.deserialize_VK_LONG8()
                raise Exception("Error code: {:d}".format(error_code))
            row_values[column.name]=value
        row = self._row_factory(**row_values)
        return row
        
    def rows(self):
        while self.row_number<self.row_count:
            if not self._rows_cache:
                first_row = self.row_count_received
                page_size = self.connection.res_size
                last_row=min(first_row+page_size, self.row_count-1)
                self._fetch(first_row=first_row, 
                    last_row=last_row)
            yield self._rows_cache.popleft()
            self.row_number+=1

    def read_row(self):
        try:
            row = self.rows().__next__()
            self.row_number+=1
        except StopIteration:
            row = None
        return row

    def _fetch(self, command_index=None,first_row=None,last_row=None):
        statement_cmd = FourDFetchStatement(statement_id=self.statement_id,
            command_index=command_index or 0, 
            first_row_index=first_row, 
            last_row_index=last_row,
            output_mode='Release',
            full_error_stack=True)
        self.connection._socket_send(statement_cmd)
        header_bytes = self._read_header_bytes()
        status_line, header_lines = self._get_header_lines(header_bytes)
        status_code, statement_code = self._decode_status(status_line)
        if not status_code == OK:
            raise Exception("Error: error in fetch\n")
        for i in range(last_row-first_row+1):
                self._rows_cache.append(self._read_row())

    def _read_value(self, column):
        if not column.dtype in fourD_str_types:
            raise Exception('Missing data value %s'%column.dtype)
        return self.deserialize(column)
        
    def deserialize(self, column):
        dtype = column.dtype
        deserializer = getattr(self,'deserialize_{}'.format(dtype))
        return deserializer()

    def _recv(self, to_receive):
        return self.socket.recv(to_receive,socket.MSG_WAITALL)       

    def deserialize_VK_BOOLEAN(self):
        return bool(struct.unpack('<H', self._recv(2))[0])

    def deserialize_VK_LONG(self):
        return struct.unpack('<l', self._recv(4))[0]

    def deserialize_VK_WORD(self):
        return struct.unpack('<h', self._recv(2))[0]

    def deserialize_VK_LONG8(self):
        return struct.unpack('<q', self._recv(8))[0]

    def deserialize_VK_REAL(self):
        return struct.unpack('<d', self._recv(8))[0]

    def deserialize_VK_TIMESTAMP(self):
        year,month,day,millisecond = struct.unpack('<HBBL',self._recv(8))
        second = millisecond//1000
        millisecond = millisecond-second*1000
        microsecond = millisecond*1000
        minute = second//60
        second = second-minute*60
        hour = minute//60
        minute = minute-hour*60
        if not year:
            return None
        return datetime(year,month,day,hour,minute,second,microsecond)


    deserialize_VK_TIME = deserialize_VK_TIMESTAMP

    def deserialize_VK_DURATION(self):
        milliseconds = struct.unpack('<Q', self._recv(8))[0]
        second = milliseconds//1000
        microsecond = (milliseconds-second*1000)*1000
        minute = second//60
        second = second - minute*60
        hour = minute//60
        minute = minute - hour*60
        return time(hour, minute, second, microsecond)

    def deserialize_VK_STRING(self):
        str_len= -struct.unpack('<l',self._recv(4))[0]
        str_len *= 2 # UTF-16LE Strings use 2 bytes per character
        encoded_value=self._recv(str_len)
        return encoded_value.decode('UTF-16LE').encode().decode()
        
    def deserialize_VK_BLOB(self):
        blob_len = struct.unpack('<l',self._recv(4))[0]
        return self._recv(blob_len)

    def deserialize_VK_UNKNOW(self):
        pass

    def __repr__(self):
        headers_lines = [] 
        for k,v in self.headers.items():
            headers_lines.append('\t%s:%s'%(k,v))
        headers = '\n'.join(headers_lines)
        return self.statement_code+'\n'+self.status_code+'\n'+headers
    
    @property
    def OK(self):
        return self.status_code == OK

    def __getitem__(self,key):
        if isinstance(key, str):
            key=key.encode()
        value = self.headers.get(key)
        return value

    @property
    def is_result_set(self):
        return self.result_type == RESULT_SET

    @property
    def exception(self):
        code = self.headers.get('Error-Code')
        component_code = self.headers.get('Error-Component-Code')
        description = self.headers.get('Error-Description')
        return FourDException(code=code, component_code=component_code, description=description)

    @property
    def statement_id(self):
        if not hasattr(self, '_statement_id'):
            self._statement_id = self.headers.get('Statement-ID','')
            if self._statement_id:
                self._statement_id = int(self._statement_id)
        return self._statement_id

class FourDFetchResponse(FourDResponse):
    pass

class FourD:
    def __init__(self, host=None, user=None, password=None, 
            database=None, port=None, res_size=None, reply_64=True):
        self.host=host
        self.user=user
        self.password=password
        self.database=database
        self.port=port
        self.connected=False
        self.set_preferred_image_types(DEFAULT_IMAGE_TYPE)
        self.res_size = res_size or 100
        self.reply_64=reply_64
        self.current_response = None

    def set_preferred_image_types(self, types):
        self.image_type = types

    def connect(self):
        if self.connected:
            return
        self.socket = socket.create_connection((self.host,self.port), 15)
        self.dblogin()
        self.connected=True

    def fourd_send(self, bytes_value, response_factory=None):
        self._socket_send(bytes_value)
        response_factory = response_factory or FourDResponse
        return FourDResponse(self)

    def _socket_send(self, bytes_value):
        if not isinstance(bytes_value, bytes):
            bytes_value = bytes(bytes_value)
        self.socket.send(bytes_value)

    
    def dblogin(self):
        if __LOGIN_BASE64__:
            login_class = FourDLogin
        else:
            login_class = FourDLoginPlain
        login_cmd = login_class(user_name=self.user, user_password=self.password, reply_with_base_64_text=self.reply_64)
        self.fourd_send(login_cmd)

    def dblogout(self):
        self.fourd_send(FourDLogout())

    def quit(self):
        self.fourd_send(FourDQuit())

    def close(self):
        self.dblogout()
        self.quit()
        self.socket.close()
        self.connected=False

    def prepare_statement(self, statement, statement_params=None):
        if __STATEMENT_BASE64__:
            statement_class = FourDPrepareStatement 
        else:
            statement_class = FourDPrepareStatementPlain
        statement_cmd = statement_class(statement=statement, statement_params=statement_params)
        return self.fourd_send(statement_cmd)

    def execute_statement(self, statement, statement_params=None, first_page_size=0):
        if __STATEMENT_BASE64__:
            statement_class = FourDExecuteStatement 
        else:
            statement_class = FourDExecuteStatementPlain
        statement_cmd = statement_class(statement=statement, 
            first_page_size=first_page_size or 0,
            output_mode='Release',full_error_stack=True,
            statement_params=statement_params)
        result = self.fourd_send(statement_cmd)
        return result

    


