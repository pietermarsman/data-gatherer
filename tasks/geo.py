import datetime
import json
import os
from hashlib import md5

import luigi

from config import settings
from misc import sanitize_str, GoogleDrive


class GeoDrive(luigi.Task):
    query = luigi.Parameter()
    mime_type = luigi.Parameter()
    folder = luigi.Parameter()
    date = luigi.DateParameter()

    def output(self):
        dir_date = "{date:%Y/%m/%d/}".format(date=self.date)
        file_name = "%s_%s_%s.json" % (
        md5(self.query.encode('utf-8')).hexdigest(), sanitize_str(self.mime_type), sanitize_str(self.folder))
        file_path = os.path.join(settings['io']['out'], self.__class__.__name__, dir_date, file_name)
        return luigi.LocalTarget(file_path)


class ExtractGeoDrive(GeoDrive):
    def run(self):
        gd = GoogleDrive()
        query = self.query.format(folder=self.folder, mime_type=self.mime_type, date=self.date)
        result = gd.list_files(query)
        file_names = result['files']

        values = {}
        if len(file_names) > 0:
            file_id = file_names[0]['id']
            values = gd.get_file(file_id, 'utf-8')
            values = json.loads(values)

        with self.output().open('w') as f:
            json.dump(values, f)


class TransformGeo(GeoDrive):
    def requires(self):
        return [ExtractGeoDrive(query=self.query, mime_type=self.mime_type, folder=self.folder, date=self.date)]

    def run(self):
        with self.input()[0].open() as f:
            data = json.load(f)

        records = []
        for feature in data.get('features', []):
            record = {
                "lat": feature["geometry"]["coordinates"][0],
                "lon": feature["geometry"]["coordinates"][1],
                "accuracy": float(feature["properties"]["accuracy"]),
                "datetime": datetime.datetime.strptime(feature['properties']['time'],
                                                       '%Y-%m-%dT%H:%M:%S.%fZ').isoformat()
            }
            records.append(record)

        with self.output().open('w') as f:
            json.dump(records, f)


class LoadGeo(GeoDrive):
    def requires(self):
        return [TransformGeo(query=self.query, mime_type=self.mime_type, folder=self.folder, date=self.date)]

    def run(self):
        pass


class LoadAllGeo(luigi.WrapperTask):
    query = luigi.Parameter()
    mime_type = luigi.Parameter()
    folder = luigi.Parameter()
    start_date = luigi.DateParameter(default=datetime.datetime(2018, 1, 18))
    end_date = luigi.DateParameter(default=datetime.datetime.today())

    def dates(self):
        n_days = (self.end_date - self.start_date).days
        dates = [self.end_date - datetime.timedelta(days=x + 1) for x in range(n_days)]
        return dates

    def requires(self):
        return [LoadGeo(query=self.query, mime_type=self.mime_type, folder=self.folder, date=date) for date in
                self.dates()]
