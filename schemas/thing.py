from neomodel import StringProperty, config
from neomodel import StructuredNode


config.DATABASE_URL = 'bolt://neo4j:neo4j@localhost:7687'  # default


class Thing(StructuredNode):
    name = StringProperty(required=True)
