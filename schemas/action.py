from neomodel import FloatProperty
from neomodel import Relationship
from neomodel import StructuredNode

from intangible import BankAccount
from thing import Thing


class Action(Thing):
    datetime = Relationship(StructuredNode, "AT")


class TradeAction(Action):
    price = FloatProperty(required=True)


class BuyFuelAction(TradeAction):
    volume = FloatProperty(required=True)


class BankTransferAction(TradeAction):
    account = Relationship(BankAccount, 'ON_ACCOUNT')
