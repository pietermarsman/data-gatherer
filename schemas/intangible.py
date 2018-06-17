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
    next = Relationship('Measurement', "NEXT")
    value = FloatProperty(required=True)


class BankAccount(StructuredNode, Intangible):
    pass


class Geo(Intangible):
    pass


class GeoCoordinate(StructuredNode, Geo):
    datetime = Relationship(StructuredNode, "AT")
    lat = FloatProperty(required=True)
    lon = FloatProperty(required=True)


class HouseNumber(StructuredNode, Geo):
    geos = Relationship(GeoCoordinate, "IN")


class Zipcode(StructuredNode, Geo):
    house_numbers = Relationship(HouseNumber, "IN")


class Road(StructuredNode, Geo):
    geos = Relationship(GeoCoordinate, "IN")
    house_numbers = Relationship(HouseNumber, "IN")


class Town(StructuredNode, Geo):
    roads = Relationship(Road, "IN")


class State(StructuredNode, Geo):
    towns = Relationship(Town, "IN")


class Country(StructuredNode, Geo):
    states = Relationship(State, "IN")
