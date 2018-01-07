import json
import os
import subprocess
from datetime import datetime

import dateparser
import luigi
from dateutil import parser

from action import AchieveAction
from google_drive import DATA_DIR, get_time_node


class ExtractTodo(luigi.Task):
    datetime = luigi.DateParameter()

    def output(self):
        file_name = "%s.json" % self.datetime
        path = os.path.join(DATA_DIR, ExtractTodo.__name__, file_name)
        return luigi.LocalTarget(path)

    def run(self):
        self.output().makedirs()
        subprocess.run(["./dumpdata.sh", self.output().path], cwd="/Users/pieter/Documents/Projects/startpage/")


class TransformTodo(luigi.Task):
    datetime = luigi.DateParameter()

    def requires(self):
        return [ExtractTodo(self.datetime)]

    def output(self):
        file_name = "%s.json" % self.datetime
        path = os.path.join(DATA_DIR, TransformTodo.__name__, file_name)
        return luigi.LocalTarget(path)

    def run(self):
        with self.input()[0].open() as f:
            records = json.load(f)

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

        with self.output().open('w') as f:
            json.dump(todos, f, sort_keys=True, indent=4)


class LoadTodo(luigi.Task):
    datetime = luigi.DateParameter()

    def requires(self):
        return [TransformTodo(self.datetime)]

    def output(self):
        file_name = "%s.log" % self.datetime
        path = os.path.join(DATA_DIR, LoadTodo.__name__, file_name)
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


class LoadTodoInGraph(luigi.WrapperTask):

    def requires(self):
        return [LoadTodo(datetime.now().date())]
