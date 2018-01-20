from __future__ import print_function

import argparse
import datetime
import json
import os

import httplib2
import luigi
from apiclient import discovery
from dateutil import parser
from oauth2client import client, tools
from oauth2client.file import Storage

from config import settings
from misc import sanitize_str, get_time_node
from schemas.action import BuyFuelAction
from schemas.intangible import Measurement, Metric

SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'
CLIENT_SECRET_FILE = '../reference/client_secret.json'
APPLICATION_NAME = 'Google Sheets API Python Quickstart'


def get_google_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    credential_path = get_google_credential_path()

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
        credentials = tools.run_flow(flow, store, flags)
        print('Storing credentials to ' + credential_path)
    return credentials


def get_google_credential_path():
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir, 'sheets.googleapis.com-python-quickstart.json')
    return credential_path


def get_service():
    credentials = get_google_credentials()
    http = credentials.authorize(httplib2.Http())
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?version=v4')
    service = discovery.build('sheets', 'v4', http=http, discoveryServiceUrl=discoveryUrl)
    return service


class ExtractFuelDrive(luigi.Task):
    spreadsheet_id = luigi.Parameter()
    range = luigi.Parameter()
    date = luigi.DateParameter(batch_method=max)

    def output(self):
        """Note: dowloaded new file every day, and not depending on date parameter since new file contains everything"""
        dir_date = "{date:%Y/%m/%d/}".format(date=datetime.datetime.today())
        file_name = "%s_%s.json" % (sanitize_str(self.spreadsheet_id), sanitize_str(self.range))
        file_path = os.path.join(settings['io']['out'], ExtractFuelDrive.__name__, dir_date, file_name)
        return luigi.LocalTarget(file_path)

    def run(self):
        sheets = get_service().spreadsheets().values()
        result = sheets.get(spreadsheetId=self.spreadsheet_id, range=self.range, majorDimension="ROWS").execute()

        with self.output().open('w') as f:
            json.dump(result, f)


class TransformFuelDrive(luigi.Task):
    spreadsheet_id = luigi.Parameter()
    range = luigi.Parameter()
    date = luigi.DateParameter(batch_method=max)

    def output(self):
        dir_date = "{date:%Y/%m/%d/}".format(date=self.date)
        file_name = "%s_%s.json" % (sanitize_str(self.spreadsheet_id), sanitize_str(self.range))
        file_path = os.path.join(settings['io']['out'], TransformFuelDrive.__name__, dir_date, file_name)
        return luigi.LocalTarget(file_path)

    def run(self):
        with self.input()[0].open() as f:
            data = json.load(f)

        values = data.get('values', [])
        records = [{k: v for k, v in zip(values[0], values_iter)} for values_iter in values[1:]]
        for record in records:
            record['Timestamp'] = datetime.datetime.strptime(record['Timestamp'], "%d/%m/%Y %H:%M:%S").isoformat()
            record['Aantal liter'] = float(record['Aantal liter'])
            record['Prijs'] = float(record['Prijs'])
            record['Kilometerstand'] = float(record['Kilometerstand'])

        records = [record for record in records if record['Timestamp'].startswith(self.date.isoformat())]

        with self.output().open('w') as f:
            json.dump(records, f)


class LoadFuelDrive(luigi.Task):
    spreadsheet_id = luigi.Parameter()
    range = luigi.Parameter()
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return [ExtractFuelDrive(spreadsheet_id=self.spreadsheet_id, range=self.range, date=self.date)]

    def output(self):
        dir_date = "{date:%Y/%m/%d/}".format(date=self.date)
        file_name = "%s_%s.log" % (sanitize_str(self.spreadsheet_id), sanitize_str(self.range))
        file_path = os.path.join(settings['io']['out'], LoadFuelDrive.__name__, dir_date, file_name)
        return luigi.LocalTarget(file_path)

    # noinspection PyTypeChecker
    def run(self):
        with self.input()[0].open() as f:
            data = json.load(f)

        with self.output().open('w') as f:
            f.write('Read %d records' % len(data))

        for record in data:
            dt_node = get_time_node(parser.parse(record['Timestamp']))

            action = BuyFuelAction.get_or_create({
                "name": "%.2f EUR (%s)" % (record['Prijs'], record['Timestamp']),
                "price": record['Prijs'],
                "volume": record['Aantal liter']
            })[0]
            action.datetime.connect(dt_node)

            metric = Metric.get_or_create({
                "name": "Mileage Daihatsu Cuore",
                "unit": "km"
            })[0]
            measurement = Measurement.get_or_create({
                "name": "%d" % record["Kilometerstand"],
                "value": record["Kilometerstand"]
            })[0]
            measurement.metric.connect(metric)
            measurement.datetime.connect(dt_node)


class LoadAllFuelDrive(luigi.WrapperTask):
    spreadsheet_id = luigi.Parameter()
    range = luigi.Parameter()
    start_date = luigi.DateParameter(default=datetime.datetime(2017, 10, 1))
    end_date = luigi.DateParameter(default=datetime.datetime.today())

    def dates(self):
        n_days = (self.end_date - self.start_date).days
        dates = [self.end_date - datetime.timedelta(days=x + 1) for x in range(n_days)]
        return dates

    def requires(self):
        return [LoadFuelDrive(spreadsheet_id=self.spreadsheet_id, range=self.range, date=date) for date in
                self.dates()]
