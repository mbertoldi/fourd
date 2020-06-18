
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

class Warning(FourDException):
    pass

class Error(FourDException):
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
