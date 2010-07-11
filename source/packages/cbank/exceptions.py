"""Exceptional conditions for cbank."""

__all__ = ["ClusterbankException", "NotFound", "InsufficientFunds"]

class ClusterbankException (Exception):
    """Base exception for cbank."""

class NotFound (ClusterbankException):
    """The specified entity was not found."""

class InsufficientFunds (ClusterbankException):
    """An entity has insufficient funds."""

