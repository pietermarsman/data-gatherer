from neomodel import StructuredNode, IntegerProperty
import neomodel.core


class Year(StructuredNode):
    value = IntegerProperty(unique_index=True, required=True)


class Month(StructuredNode):
    value = IntegerProperty(unique_index=True, required=True)


class Day(StructuredNode):
    value = IntegerProperty(unique_index=True, required=True)


class Hour(StructuredNode):
    value = IntegerProperty(unique_index=True, required=True)


class Minute(StructuredNode):
    value = IntegerProperty(unique_index=True, required=True)


class Second(StructuredNode):
    value = IntegerProperty(unique_index=True, required=True)


def get_time_class(name):
    return {
        "second": Second,
        "minute": Minute,
        "hour": Hour,
        "day": Day,
        "month": Month,
        "year": Year
    }.get(name.lower())