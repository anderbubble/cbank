import ConfigParser

from clusterbank import config

__all__ = ["get_unit_factor"]

def get_unit_factor ():
    try:
        factor = config.get("cbank", "unit_factor")
    except ConfigParser.Error:
        factor = "1"
    try:
        mul, div = factor.split("/")
    except ValueError:
        mul = factor
        div = 1
    try:
        mul = float(mul)
        div = float(div)
    except ValueError:
        warnings.warn("invalid unit factor: %s" % factor)
        mul = 1
        div = 1
    return mul, div

