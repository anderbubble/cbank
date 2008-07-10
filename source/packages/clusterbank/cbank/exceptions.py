"""Exceptional cases and errors in the cbank interface."""

__all__ = ["CbankException", "CbankError", "UnknownUser"]


class CbankException (Exception):
    
    """Base class for exceptions in the cbank interface."""
    
    exit_code = 0
    
    def __str__ (self):
        if not self.args:
            return "cbank: unknown exception"
        else:
            return "cbank: %s" % self.args[0]


class CbankError (CbankException):
    
    """Base class for errors in the cbank interface."""
    
    exit_code = -1
    
    def __str__ (self):
        if not self.args:
            return "cbank: unknown error"
        else:
            return "cbank: error: %s" % self.args[0]
        

class UnknownUser (CbankError):
    
    """The specified user is not present in the system."""
    
    exit_code = -2
    
    def __str__ (self):
        if not self.args:
            return "cbank: unknown user"
        else:
            return "cbank: unknown user: %s" % self.args[0]


class UnexpectedArguments (CbankError):
    
    """Unexpected arguments were passed to the script."""
    
    exit_code = -3
    
    def __str__ (self):
        if not self.args:
            return "cbank: unexpected arguments"
        else:
            try:
                args = ", ".join(self.args[0])
            except TypeError:
                args = self.args[0]
            return "cbank: unexpected arguments: %s" % args


class UnknownCommand (CbankError):
    
    """A metacommand received an invalid command string."""
    
    exit_code = -4
    
    def __str__ (self):
        if not self.args:
            return "cbank: unknown command"
        else:
            return "cbank: unknown command: %s" % self.args[0]


class NotPermitted (CbankError):
    
    """The requested action is not permitted by the specified user."""
    
    exit_code = -5
    
    def __str__ (self):
        if not self.args:
            return "cbank: not permitted"
        else:
            return "cbank: not permitted: %s" % self.args[0]


class MissingOption (CbankError):
    
    exit_code = -6
    
    def __str__ (self):
        if not self.args:
            return "cbank: missing option"
        else:
            return "cbank: missing option: %s" % self.args[0]


class ValueError (CbankError):
    
    exit_code = -7
    
    def __str__ (self):
        if not self.args:
            return "cbank: value error"
        else:
            return "cbank: value error: %s" % self.args[0]


class MissingCommand (CbankError):
    
    exit_code = -8
    
    def __str__ (self):
        if not self.args:
            return "cbank: missing command"
        else:
            return "cbank: missing command: %s" % self.args[0]
