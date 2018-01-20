import re
from datetime import datetime

import pytz
from neomodel import db, StructuredNode


def sanitize_str(s):
    return re.sub("\W", "_", s).lower()


def get_time_node(dt, resolution="Day"):
    dt = dt.astimezone(pytz.utc)
    c = dt - datetime(1970, 1, 1).astimezone(pytz.utc)
    t = int((c.days * 24 * 60 * 60 + c.seconds) * 1000 + c.microseconds / 1000.0)
    query = "CALL ga.timetree.single({time: %s, create: true, resolution: \"%s\"})" % (t, resolution)
    results, meta = db.cypher_query(query)
    node = StructuredNode.inflate(results[0][0])
    return node
