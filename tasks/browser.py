import datetime
import json
import os
import sqlite3
import subprocess

import luigi
import pytz
from dateutil import parser

from config import settings
from misc import get_time_node
from schemas.action import ViewAction, Domain


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class ExtractBrowserHistory(luigi.Task):
    date = luigi.DateParameter(batch_method=max)
    db_location = luigi.Parameter(default='/Users/pieter/Library/Safari/History.db')

    def output(self):
        """Note: dowloaded new file every day, and not depending on date parameter since new file contains everything"""
        file_path = "{date:%Y/%m/%d}.sqlite".format(date=datetime.datetime.today())
        path = os.path.join(settings['io']['out'], ExtractBrowserHistory.__name__, file_path)
        return luigi.LocalTarget(path)

    def run(self):
        self.output().makedirs()
        ret = subprocess.run(['cp', self.db_location, self.output().path])


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
    date = luigi.DateParameter()

    def requires(self):
        return [ExtractBrowserHistory(self.date)]

    def output(self):
        file_path = "{date:%Y/%m/%d}.json".format(date=self.date)
        path = os.path.join(settings['io']['out'], TransformBrowserHistory.__name__, file_path)
        return luigi.LocalTarget(path)

    def run(self):
        with sqlite3.connect(self.input()[0].path) as con:
            con.row_factory = dict_factory
            cur = con.cursor()
            query = self.QUERY % self.date
            records = cur.execute(query).fetchall()

            records = [{k: v for k, v in record.items() if v is not None} for record in records]
            for record in records:
                dt = datetime.datetime.strptime(record['visit_datetime'], '%Y-%m-%d %H:%M:%S')
                record['visit_datetime'] = dt.astimezone(pytz.utc).isoformat()

            records = [record for record in records if record['visit_datetime'].startswith(self.date.isoformat())]

            with self.output().open('w') as f:
                json.dump(records, f, sort_keys=True, indent=4)


class LoadBrowserHistory(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return [TransformBrowserHistory(self.date)]

    def output(self):
        file_path = "{date:%Y/%m/%d}.log".format(date=self.date)
        path = os.path.join(settings['io']['out'], LoadBrowserHistory.__name__, file_path)
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


class LoadAllBrowserHistory(luigi.WrapperTask):
    start_date = luigi.DateParameter(default=datetime.datetime(2017, 3, 1))
    end_date = luigi.DateParameter(default=datetime.datetime.today())

    def dates(self):
        n_days = (self.end_date - self.start_date).days
        dates = [self.end_date - datetime.timedelta(days=x + 1) for x in range(n_days)]
        return dates

    def requires(self):
        return [LoadBrowserHistory(date) for date in self.dates()]
