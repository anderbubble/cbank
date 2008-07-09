class CbankException (Exception): pass

class CbankError (CbankException): pass

class UnknownUser (CbankError):
    
    def __str__ (self):
        return "cbank: unknown user: %s" % CbankError.__str__(self)

class UnknownReport (CbankError):
    
    def __str__ (self):
        return "cbank: unknown report: %s" % CbankError.__str__(self)

class UnknownCommand (CbankError):
    
    def __str__ (self):
        return "cbank: unknown command: %s" % CbankError.__str__(self)

class MisusedOption (CbankError):
    
    def __str__ (self):
        return "cbank: misused option: %s" % CbankError.__str__(self)

class NotPermitted (CbankError):
    
    def __str__ (self):
        return "cbank: not permitted: %s" % CbankError.__str__(self)

class MissingOption (CbankError):
    
    def __str__ (self):
        return "cbank: missing option: %s" % CbankError.__str__(self)

class InvalidOptionValue (CbankError):
    
    def __str__ (self):
        return "cbank: invalid option value: %s" % CbankError.__str__(self)

