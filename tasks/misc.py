import argparse
import os
import re
import io
from datetime import datetime

import httplib2
import pytz
from googleapiclient import discovery
from googleapiclient.http import MediaIoBaseDownload
from neomodel import db, StructuredNode
from oauth2client import client, tools
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.file import Storage

from config import settings


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


class GoogleDrive(object):
    def __init__(self):
        self.scopes = 'https://www.googleapis.com/auth/drive'
        self.redirect_url = 'urn:ietf:wg:oauth:2.0:oob'
        self.application_name = 'Google Sheets API Python Quickstart'
        self.discovery_url = 'https://sheets.googleapis.com/$discovery/rest?version=v4'
        self.major_dimension = "ROWS"
        self.service_name = 'sheets'
        self.version = 'v4'

        self.drive_service = None
        self.service = None

    def get_spreadsheet_values(self, spreadsheet_id, range):
        values = self.get_service(). \
            spreadsheets(). \
            values(). \
            get(spreadsheetId=spreadsheet_id, range=range, majorDimension=self.major_dimension). \
            execute()
        return values

    def list_files(self, query):
        files = self.get_drive_service().files().list(q=query).execute()
        return files

    def get_file(self, file_id, decode=None):
        content = self.get_drive_service().\
            files().get_media(fileId=file_id).\
            execute()
        if decode is not None:
            content = content.decode(decode)
        return content

    def get_service(self):
        if self.service is None:
            credentials = self._get_google_credentials2()
            http = credentials.authorize(httplib2.Http())
            self.service = discovery.build(self.service_name, self.version, http=http, discoveryServiceUrl=self.discovery_url)
        return self.service

    def get_drive_service(self):
        if self.drive_service is None:
            credentials = self._get_google_credentials2()
            http = credentials.authorize(httplib2.Http())
            self.drive_service = discovery.build('drive', 'v3', http=http)
        return self.drive_service

    def _get_google_credentials2(self):
        storage = Storage(self._get_google_credential_path())
        credentials = storage.get()

        if credentials is None:
            # Run through the OAuth flow and retrieve credentials
            flow = OAuth2WebServerFlow(settings['google_drive']['client_id'], settings['google_drive']['client_secret'], self.scopes, self.redirect_url)
            authorize_url = flow.step1_get_authorize_url()
            print('Go to the following link in your browser: ' + authorize_url)
            code = input('Enter verification code: ').strip()
            credentials = flow.step2_exchange(code)
            storage.put(credentials)

        return credentials

    def _get_google_credential_path(self):
        home_dir = os.path.expanduser('~')
        credential_dir = os.path.join(home_dir, '.credentials')
        if not os.path.exists(credential_dir):
            os.makedirs(credential_dir)
        credential_path = os.path.join(credential_dir, 'sheets.googleapis.com-python-quickstart.json')
        return credential_path
