from neomodel import StringProperty


class Thing(object):
    name = StringProperty(required=True)
