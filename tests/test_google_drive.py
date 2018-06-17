import datetime
import unittest

from googleapiclient.discovery import Resource
from neomodel import config, db

from config import settings
from misc import sanitize_str, get_time_node, GoogleDrive

db.set_connection('bolt://%s:%s@localhost:7687' % (settings['neo4j']['user'], settings['neo4j']['password']))


class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.google_drive = GoogleDrive()

    def test_sanitize_string(self):
        text = sanitize_str('What drives you?')
        self.assertEqual(text, 'what_drives_you_')

    def test_credential_path(self):
        path = GoogleDrive._get_google_credential_path()
        self.assertEqual(type(path), str)
        self.assertGreater(len(path), 0)

    def test_drive_service(self):
        service = self.google_drive.get_service()
        self.assertEqual(type(service), Resource)

    def test_credentials(self):
        credentials = self.google_drive._get_google_credentials2()
        credentials.get_access_token()
        self.assertFalse(credentials.access_token_expired)

    def test_time_node(self):
        dt = datetime.datetime.now()
        node = get_time_node(dt, "minute")
        self.assertEqual(dt.minute, node.value)


if __name__ == '__main__':
    unittest.main()
