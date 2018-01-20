import argparse
import os
import re
from datetime import datetime

import httplib2
import pytz
from googleapiclient import discovery
from neomodel import db, StructuredNode
from oauth2client import client, tools
from oauth2client.file import Storage


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


class GoogleDriveSpreadSheet(object):
    def __init__(self, spreadsheet_id, range):
        self.scopes = 'https://www.googleapis.com/auth/spreadsheets.readonly'
        self.client_secret_file = '../reference/client_secret.json'
        self.application_name = 'Google Sheets API Python Quickstart'
        self.discovery_url = 'https://sheets.googleapis.com/$discovery/rest?version=v4'
        self.major_dimension = "ROWS"
        self.service_name = 'sheets'
        self.version = 'v4'

        self.spreadsheet_id = spreadsheet_id
        self.range = range

    def get_values(self):
        values = self.get_service(). \
            spreadsheets(). \
            values(). \
            get(spreadsheetId=self.spreadsheet_id, range=self.range, majorDimension=self.major_dimension). \
            execute()
        return values

    def get_service(self):
        credentials = self._get_google_credentials()
        http = credentials.authorize(httplib2.Http())
        service = discovery.build(self.service_name, self.version, http=http, discoveryServiceUrl=self.discovery_url)
        return service

    def _get_google_credentials(self):
        """Gets valid user credentials from storage.

        If nothing has been stored, or if the stored credentials are invalid,
        the OAuth2 flow is completed to obtain the new credentials.

        Returns:
            Credentials, the obtained credential.
        """
        credential_path = self._get_google_credential_path()

        store = Storage(credential_path)
        credentials = store.get()
        if not credentials or credentials.invalid:
            flow = client.flow_from_clientsecrets(self.client_secret_file, self.scopes)
            flow.user_agent = self.application_name
            flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
            credentials = tools.run_flow(flow, store, flags)
            print('Storing credentials to ' + credential_path)
        return credentials

    def _get_google_credential_path(self):
        home_dir = os.path.expanduser('~')
        credential_dir = os.path.join(home_dir, '.credentials')
        if not os.path.exists(credential_dir):
            os.makedirs(credential_dir)
        credential_path = os.path.join(credential_dir, 'sheets.googleapis.com-python-quickstart.json')
        return credential_path
