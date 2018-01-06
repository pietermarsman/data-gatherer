from __future__ import print_function

import argparse
import json
import os
import re
from datetime import datetime

import httplib2
import luigi
from apiclient import discovery
from dateutil import parser
from neomodel import db, StructuredNode
from oauth2client import client, tools
from oauth2client.file import Storage

from action import BuyFuelAction
from intangible import Measurement, CarKilometers

SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'
CLIENT_SECRET_FILE = '../reference/client_secret.json'
APPLICATION_NAME = 'Google Sheets API Python Quickstart'

DATA_DIR = os.path.join(os.path.expanduser('~'), "Data", "personal")


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


def sanitize_str(s):
    return re.sub("\W", "_", s).lower()


def get_time_node(dt, resolution="Minute"):
    c = dt - datetime(1970, 1, 1)
    t = int((c.days * 24 * 60 * 60 + c.seconds) * 1000 + c.microseconds / 1000.0)
    query = "CALL ga.timetree.single({time: %s, create: true, resolution: \"%s\"})" % (t, resolution)
    results, meta = db.cypher_query(query)
    return results[0][0]


class DownloadFuelFromDrive(luigi.Task):
    spreadsheet_id = luigi.Parameter()
    range = luigi.Parameter()

    def output(self):
        month_name = "%.4d%.2d" % (datetime.now().year, datetime.now().month)
        sheet_name = "%s_%s" % (sanitize_str(self.spreadsheet_id), sanitize_str(self.range))
        file_name = "%s_%s.json" % (month_name, sheet_name)
        dir = os.path.join(DATA_DIR, "DownloadFuelFromDrive", file_name)
        return luigi.LocalTarget(dir)

    def run(self):
        sheets = get_service().spreadsheets().values()
        result = sheets.get(spreadsheetId=self.spreadsheet_id, range=self.range, majorDimension="ROWS").execute()

        values = result.get('values', [])
        data = [{k: v for k, v in zip(values[0], values_iter)} for values_iter in values[1:]]
        for record in data:
            record['Timestamp'] = datetime.strptime(record['Timestamp'], "%d/%m/%Y %H:%M:%S").isoformat()
            record['Aantal liter'] = float(record['Aantal liter'])
            record['Prijs'] = float(record['Prijs'])
            record['Kilometerstand'] = float(record['Kilometerstand'])

        with self.output().open('w') as f:
            json.dump(data, f)


class LoadFuelInGraph(luigi.Task):
    spreadsheet_id = luigi.Parameter()
    range = luigi.Parameter()

    def requires(self):
        return [DownloadFuelFromDrive(spreadsheet_id=self.spreadsheet_id, range=self.range)]

    def output(self):
        path = os.path.join(DATA_DIR, "load_fuel_in_graph.log")
        return luigi.LocalTarget(path)

    def run(self):
        with self.input()[0].open() as f:
            data = json.load(f)

        with self.output().open('w') as f:
            f.write('Read %d records' % len(data))

        for record in data:
            raw_dt_node = get_time_node(parser.parse(record['Timestamp']))
            dt_node = StructuredNode.inflate(raw_dt_node)

            action = BuyFuelAction.get_or_create({
                    "price": record['Prijs'],
                    "volume": record['Aantal liter']
            })[0]
            action.datetime.connect(dt_node)

            metric = CarKilometers.get_or_create({})[0]
            measurement = Measurement.get_or_create({
                "value": record["Kilometerstand"]
            })[0]
            measurement.metric.connect(metric)
            measurement.datetime.connect(dt_node)



if __name__ == "__main__":
    try:
        os.remove("/Users/pieter/Data/personal/load_fuel_in_graph.log")
    except:
        pass
    luigi.run()
