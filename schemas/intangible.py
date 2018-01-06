from neomodel import DateTimeProperty, StructuredNode
from neomodel import FloatProperty
from neomodel import Relationship
from neomodel import StringProperty

from thing import Thing


class Intangible(Thing):
    pass


class Metric(Intangible):
    unit = StringProperty()


class CarKilometers(Metric):
    unit = StringProperty(default="km")


class Measurement(Intangible):
    datetime = Relationship(StructuredNode, "AT")
    metric = Relationship(Metric, "MEASURED")
    value = FloatProperty(required=True)
