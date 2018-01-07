from neomodel import StringProperty, config

config.DATABASE_URL = 'bolt://neo4j:neo4j@localhost:7687'  # default


class Thing(object):
    name = StringProperty(required=True)
