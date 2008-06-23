class CbankException (Exception): pass

class CbankError (CbankException): pass

class UnknownUser (CbankError):
    
    def __str__ (self):
        return "cbank: unknown user: %s" % CbankError.__str__(self)

class UnknownReport (CbankError):
    
    def __str__ (self):
        return "cbank: unknown report: %s" % CbankError.__str__(self)

class MisusedOption (CbankError):
    
    def __str__ (self):
        return "cbank: misused option: %s" % CbankError.__str__(self)
