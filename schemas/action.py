from neomodel import FloatProperty
from neomodel import Relationship
from neomodel import StructuredNode

from intangible import BankAccount
from thing import Thing


class Action(Thing):
    datetime = Relationship(StructuredNode, "AT")


class TradeAction(Action):
    price = FloatProperty(required=True)


class BuyFuelAction(StructuredNode, TradeAction):
    volume = FloatProperty(required=True)


class BankTransferAction(StructuredNode, TradeAction):
    account = Relationship(BankAccount, 'ON_ACCOUNT')


class AchieveAction(StructuredNode, Action):
    pass
