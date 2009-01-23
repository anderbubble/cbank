"""Exceptional cases and errors in the cbank interface.

Each cbank exception has an associated exit_code class attribute.

Exceptions:
CbankException -- a generic exception (0)
CbankError -- a generic error (-1)
UnknownEntity -- an entity could not be found (-2)
UnknownUser -- a user could not be found (-2)
UnknownProject -- a project could not be found (-2)
UnknownAllocation -- an allocation could not be found (-2)
UnknownCharge -- a charge could not be found (-2)
MissingArgument -- a required argument is missing (-3)
UnexpectedArguments -- unexpected arguments were found (-4)
UnknownCommand -- an unknown dispatch command was specified (-5)
NotPermitted -- the specified action is not permitted (-6)
MissingResource -- no resource was specified (-7)
ValueError_ (-8) -- wrapper for the ValueError builtin (-8)
MissingCommand -- a required dispatch command was not specified (-9)
"""

__all__ = ["CbankException", "CbankError", "UnknownEntity", "UnknownUser",
    "UnknownProject", "UnknownAllocation", "UnknownCharge", "MissingArgument",
    "UnexpectedArguments", "UnknownCommand", "NotPermitted",
    "MissingResource", "ValueError_", "MissingCommand"]


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


class UnknownEntity (CbankError):
    
    """The specified entity is not present in the system."""
    
    exit_code = -2
        

class UnknownUser (UnknownEntity):
    
    """The specified user is not present in the system."""
    
    def __str__ (self):
        if not self.args:
            return "cbank: unknown user"
        else:
            return "cbank: unknown user: %s" % self.args[0]


class UnknownProject (UnknownEntity):
    
    """The specified project is not present in the system."""
    
    def __str__ (self):
        if not self.args:
            return "cbank: unknown project"
        else:
            return "cbank: unknown project: %s" % self.args[0]


class UnknownAllocation (UnknownEntity):
    
    """The specified allocation is not present in the system."""
    
    def __str__ (self):
        if not self.args:
            return "cbank: unknown allocation"
        else:
            return "cbank: unknown allocation: %s" % self.args[0]


class UnknownCharge (UnknownEntity):
    
    """The specified charge is not present in the system."""
    
    def __str__ (self):
        if not self.args:
            return "cbank: unknown charge"
        else:
            return "cbank: unknown charge: %s" % self.args[0]


class MissingArgument (CbankError):
    
    """An expected argument was not passed to the script."""
    
    exit_code = -3
    
    def __str__ (self):
        if not self.args:
            return "cbank: missing argument"
        else:
            return "cbank: missing argument: %s" % self.args[0]


class UnexpectedArguments (CbankError):
    
    """Unexpected arguments were passed to the script."""
    
    exit_code = -4
    
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
    
    exit_code = -5
    
    def __str__ (self):
        if not self.args:
            return "cbank: unknown command"
        else:
            return "cbank: unknown command: %s" % self.args[0]


class NotPermitted (CbankError):
    
    """The requested action is not permitted by the specified user."""
    
    exit_code = -6
    
    def __str__ (self):
        if not self.args:
            return "cbank: not permitted"
        else:
            return "cbank: not permitted: %s" % self.args[0]


class MissingResource (CbankError):
    
    """No resource could be determined where one was required."""
    
    exit_code = -7
    
    def __str__ (self):
        return "cbank: missing resource"


class ValueError_ (CbankError):
    
    """A specified value is invalid.
    
    This exception is a wrapper for the ValueError builtin, with an exit_code.
    """
    
    exit_code = -8
    
    def __str__ (self):
        if not self.args:
            return "cbank: value error"
        else:
            return "cbank: value error: %s" % self.args[0]


class MissingCommand (CbankError):
    
    """No dispatch command was specified where one was required."""
    
    exit_code = -9
    
    def __str__ (self):
        if not self.args:
            return "cbank: missing command"
        else:
            return "cbank: missing command: %s" % self.args[0]

