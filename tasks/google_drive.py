from __future__ import print_function

import argparse
import json
import os
import re

import dateparser as dateparser
import httplib2
import luigi
from apiclient import discovery
from oauth2client import client, tools
from oauth2client.file import Storage

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


def sanitize_str(s):
    return re.sub("\W", "_", s)


class FuelTask(luigi.Task):
    spreadsheet_id = luigi.Parameter()
    range = luigi.Parameter()

    def output(self):
        dir = os.path.join("/", "Users", "pieter", "Data", "personal")
        path = os.path.join(dir, "FuelTask_%s_%s.json" % (sanitize_str(self.spreadsheet_id), sanitize_str(self.range)))
        return luigi.LocalTarget(path)

    def run(self):
        sheets = get_service().spreadsheets().values()
        result = sheets.get(spreadsheetId=self.spreadsheet_id, range=self.range, majorDimension="ROWS").execute()

        values = result.get('values', [])
        data = [{k: v for k, v in zip(values[0], values_iter)} for values_iter in values[1:]]
        for record in data:
            record['Timestamp'] = dateparser.parse(record['Timestamp']).isoformat()
            record['Aantal liter'] = float(record['Aantal liter'])
            record['Prijs'] = float(record['Prijs'])
            record['Kilometerstand'] = float(record['Kilometerstand'])

        with self.output().open('w') as f:
            json.dump(data, f)




if __name__ == "__main__":
    luigi.run()
