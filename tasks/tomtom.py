import json

import luigi
import os
import datetime

from glob import glob

from dateutil import parser

from config import settings
from intangible import Metric, Measurement
from misc import get_time_node


class ExtractTrackingFile(luigi.ExternalTask):
    path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.path)


class TransformHeartRate(luigi.Task):
    path = luigi.Parameter()

    def requires(self):
        return ExtractTrackingFile(path=self.path)

    def output(self):
        file_name = "%s.json" % os.path.basename(self.path).split('.')[0]
        dir = os.path.join(settings['io']['out'], self.__class__.__name__, file_name)
        return luigi.LocalTarget(dir)

    def run(self):
        with self.input().open() as f:
            data = json.load(f)

        start_date = datetime.datetime.strptime(self.remove_last_colon(data['start_datetime']), "%Y-%m-%dT%H:%M:%S%z")
        heart_rates = data['heartrate_detail']

        measurements = []
        for time, heart_rate in zip(heart_rates['time'], heart_rates['heartrate']):
            measurements.append({
                'datetime': (start_date + datetime.timedelta(seconds=time)).isoformat(),
                'heart_rate': heart_rate
            })

        with self.output().open('w') as f:
            json.dump(measurements, f)

    @staticmethod
    def remove_last_colon(s):
        ss = s.split(':')
        if len(ss) > 1:
            return ':'.join(ss[:-1]) + ss[-1]
        else:
            return ss[0]


class LoadHeartRate(luigi.Task):
    path = luigi.Parameter()

    def requires(self):
        return TransformHeartRate(path=self.path)

    def output(self):
        file_name = "%s.json" % os.path.basename(self.path).split('.')[0]
        dir = os.path.join(settings['io']['out'], self.__class__.__name__, file_name)
        return luigi.LocalTarget(dir)

    def run(self):
        with self.input().open() as f:
            data = json.load(f)

        metric = Metric.get_or_create({
            "name": "Heart rate",
            "unit": "beats/minute"
        })[0]
        previous = None
        for record in data:
            dt_node = get_time_node(parser.parse(record['datetime']), "second")

            measurement = Measurement.create({
                "name": "%d" % record["heart_rate"],
                "value": record["heart_rate"]
            })[0]
            measurement.metric.connect(metric)
            measurement.datetime.connect(dt_node)

            if previous is not None:
                previous.next.connect(measurement)
            previous = measurement

        with self.output().open('w') as f:
            f.write('Read %d records' % len(data))


class LoadTracking(luigi.WrapperTask):
    path = luigi.Parameter()

    def requires(self):
        return LoadHeartRate(path=self.path)


class LoadAllTracking(luigi.WrapperTask):
    directory = luigi.Parameter()

    def requires(self):
        files = glob(os.path.join(self.directory, '*', 'tracking*.json_2'))
        tasks = [LoadTracking(f) for f in files]
        return tasks


class LoadAllTomTomData(luigi.WrapperTask):
    directory = luigi.Parameter()

    def requires(self):
        return LoadAllTracking(directory=self.directory)