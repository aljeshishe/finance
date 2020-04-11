from datetime import datetime, timedelta
import collections

__author__ = 'alexey.grachev'

class EnumItem:
    """
    Helper class for creating enum items with propper printing
    Example:

    .. code-block:: python


        apple = EnumItem(0, 'apple')
        orange = EnumItem(1, 'orange')
        print orange, apple
        print int(orange), int(apple)
    """
    def __init__(self, value, name):
        self.name = name
        self.value = value

    def __int__(self):
        return self.value

    def __str__(self):
        return self.name

    def __repr__(self):
        return str(self)


def as_string(date):
    return date.strftime('%d-%m-%Y')


def as_date(dateStr):
    return datetime.strptime(dateStr, '%d-%m-%Y')


def dates(startDate, count):
    return [startDate + timedelta(days=x) for x in range(0, count)]


def as_list(arg):
    return arg if isinstance(arg, (list, tuple)) else (arg,)


def shorten(items, size=3):
    return [item.name[:size] for item in items]


def date_range(start, num_days):
    return [start + timedelta(days=x) for x in range(num_days)]