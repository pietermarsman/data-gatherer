# import sqlite3
#
# import os
#
# BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# db_path = os.path.join(BASE_DIR, "../History.db")
# print(db_path)
# with sqlite3.connect(db_path) as con:
#
#     cur = con.cursor()
#
#     query = """
#     SELECT history_visits.id, history_visits.history_item, history_visits.visit_time, history_visits.title,
#       history_visits.redirect_source, history_items.url, history_items.domain_expansion
#     FROM history_visits
#     LEFT JOIN history_items ON history_visits.history_item = history_items.id
#     ORDER BY history_visits.visit_time;"""
#
#     cur.execute(query)
#     print(cur.fetchall())
import datetime
import json
import sqlite3

import luigi
import os
import subprocess

import pytz

from action import ViewAction, Domain
from dateutil import parser
from google_drive import DATA_DIR, get_time_node
from intangible import Metric


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class ExtractBrowserHistory(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())
    db_location = luigi.Parameter(default='/Users/pieter/Library/Safari/History.db')

    def output(self):
        path = os.path.join(DATA_DIR, "ExtractBrowserHistory", "{date:%Y/%m/%d}".format(date=self.date), "safari.sqlite")
        return luigi.LocalTarget(path)

    def run(self):
        self.output().makedirs()
        subprocess.run(['cp', self.db_location, self.output().path])



class TransformBrowserHistory(luigi.Task):
    QUERY = """
        SELECT history_visits.id, 
          history_visits.history_item, 
          datetime(visit_time + 978307200, 'unixepoch', 'localtime') AS visit_datetime, 
          history_visits.title,
          history_visits.redirect_source, 
          history_items.url, 
          history_items.domain_expansion
        FROM history_visits
        LEFT JOIN history_items 
        ON history_visits.history_item = history_items.id
        WHERE date(visit_datetime) = date('%s')
        ORDER BY history_visits.visit_time;
    """
    date = luigi.DateParameter(default=datetime.datetime(2018, 1, 10)) #datetime.date.today())

    def requires(self):
        return [ExtractBrowserHistory(self.date)]

    def output(self):
        path = os.path.join(DATA_DIR, "TransformBrowserHistory", "{date:%Y/%m/%d}".format(date=self.date), "safari.json")
        return luigi.LocalTarget(path)

    def run(self):
        print(self.input()[0].path)
        with sqlite3.connect(self.input()[0].path) as con:
            con.row_factory = dict_factory
            cur = con.cursor()
            query = self.QUERY % self.date
            print(query)
            records = cur.execute(self.QUERY).fetchall()
            print(records)

            records = [{k: v for k, v in record.items() if v is not None} for record in records]
            for record in records:
                record['visit_datetime'] = datetime.datetime.strptime(record['visit_datetime'], '%Y-%m-%d %H:%M:%S'). \
                    astimezone(pytz.utc).isoformat()

            with self.output().open('w') as f:
                json.dump(records, f, sort_keys=True, indent=4)



class LoadBrowserHistory(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return [TransformBrowserHistory(self.date)]

    def output(self):
        path = os.path.join(DATA_DIR, "LoadBrowserHistory", "{date:%Y/%m/%d}".format(date=self.date), "log.log")
        return luigi.LocalTarget(path)

    def run(self):
        with self.input()[0].open() as f:
            data = json.load(f)

        previous_nodes = {}
        for record in data:
            dt_node = get_time_node(parser.parse(record['visit_datetime']), 'minute')

            node = ViewAction.get_or_create({
                'name': record['id'],
                'url': record["url"],
            })[0]
            previous_nodes[record['id']] = node
            node.datetime.connect(dt_node)

            if 'domain_expansion' in record:
                domain = Domain.get_or_create({
                    'name': record['domain_expansion']
                })[0]
                node.domain.connect(domain)

            if 'redirect_source' in record:
                previous_node = previous_nodes[record['redirect_source']]
                previous_node.next.connect(node)


        with self.output().open('w') as f:
            f.write('Writen %d records' % len(data))


class CountBrowserMeasurement(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return [LoadBrowserHistory(self.date)]

    def output(self):
        path = os.path.join(DATA_DIR, "CountBrowserMeasurement", "{date:%Y/%m/%d}".format(date=self.date), "log.log")
        return luigi.LocalTarget(path)

    def run(self):
        metric = Metric.get_or_create({
            'name': 'Webpages visited',
            'unit': 'count'
        })
