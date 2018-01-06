from neomodel import DateTimeProperty
from neomodel import FloatProperty
from neomodel import Relationship
from neomodel import RelationshipTo
from neomodel import config, StructuredNode

from thing import Thing

config.DATABASE_URL = 'bolt://neo4j:neo4j@localhost:7687'  # default


class Action(Thing):
    datetime = Relationship(StructuredNode, "AT")


class TradeAction(Action):
    price = FloatProperty(required=True)


class BuyFuelAction(TradeAction):
    volume = FloatProperty(required=True)
