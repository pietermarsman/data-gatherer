import datetime
import json
import os
import subprocess

import dateparser
import luigi
from dateutil import parser

from config import settings
from misc import get_time_node
from schemas.action import AchieveAction


class ExtractTodo(luigi.Task):
    date = luigi.DateParameter(batch_method=max)

    def output(self):
        """Note: dowloaded new file every day, and not depending on date parameter since new file contains everything"""
        file_path = "{date:%Y/%m/%d}.json".format(date=datetime.datetime.today())
        path = os.path.join(settings['io']['out'], ExtractTodo.__name__, file_path)
        return luigi.LocalTarget(path)

    def run(self):
        self.output().makedirs()
        subprocess.run(["./dumpdata.sh", self.output().path], cwd="/Users/pieter/Documents/Projects/startpage/")


class TransformTodo(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return [ExtractTodo(self.date)]

    def output(self):
        file_path = "{date:%Y/%m/%d}.json".format(date=self.date)
        path = os.path.join(settings['io']['out'], TransformTodo.__name__, file_path)
        return luigi.LocalTarget(path)

    def run(self):
        with self.input()[0].open() as f:
            records = json.load(f)

        todos = self.change_format(records)
        todos = self.filter_on_date(todos)

        with self.output().open('w') as f:
            json.dump(todos, f, sort_keys=True, indent=4)

    @staticmethod
    def change_format(records):
        todos = []
        for record in records:
            if record['model'] == 'todo.todostate':
                pass

            elif record['model'] == 'todo.todo':
                if record['fields']['finished'] is not None:
                    todo = {
                        'created': dateparser.parse(record['fields']['created']).isoformat(),
                        'finished': dateparser.parse(record['fields']['finished']).isoformat(),
                        'text': record['fields']['text']
                    }
                    todos.append(todo)
        return todos

    def filter_on_date(self, todos):
        todos = [todo for todo in todos if todo['finished'].startswith(self.date.isoformat())]
        return todos


class LoadTodo(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return [TransformTodo(self.date)]

    def output(self):
        file_path = "{date:%Y/%m/%d}.log".format(date=self.date)
        path = os.path.join(settings['io']['out'], LoadTodo.__name__, file_path)
        return luigi.LocalTarget(path)

    def run(self):
        with self.input()[0].open() as f:
            records = json.load(f)

        for record in records:
            dt_node = get_time_node(parser.parse(record['finished']))
            action = AchieveAction.get_or_create({
                'name': record['text']
            })[0]
            action.datetime.connect(dt_node)

        with self.output().open('w') as f:
            f.write('Loaded %d records' % len(records))


class LoadAllTodo(luigi.WrapperTask):
    start_date = luigi.DateParameter(default=datetime.datetime(2016, 4, 1))
    end_date = luigi.DateParameter(default=datetime.datetime.today())

    def dates(self):
        n_days = (self.end_date - self.start_date).days
        dates = [self.end_date - datetime.timedelta(days=x + 1) for x in range(n_days)]
        return dates

    def requires(self):
        return [LoadTodo(date) for date in self.dates()]