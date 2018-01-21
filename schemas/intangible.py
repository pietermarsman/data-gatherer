from neomodel import FloatProperty
from neomodel import Relationship
from neomodel import StringProperty
from neomodel import StructuredNode

from thing import Thing


class Intangible(Thing):
    pass


class Metric(StructuredNode, Intangible):
    unit = StringProperty(required=True)


class Measurement(StructuredNode, Intangible):
    datetime = Relationship(StructuredNode, "AT")
    metric = Relationship(Metric, "MEASURED")
    value = FloatProperty(required=True)


class BankAccount(StructuredNode, Intangible):
    pass


class GeoCoordinate(StructuredNode, Intangible):
    datetime = Relationship(StructuredNode, "AT")
    latitude = FloatProperty(required=True)
    longitude = FloatProperty(required=True)
