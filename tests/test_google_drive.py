import datetime
import unittest

from googleapiclient.discovery import Resource

from misc import sanitize_str, get_time_node, get_google_credentials, get_google_credential_path, get_service


class MyTestCase(unittest.TestCase):
    def test_sanitize_string(self):
        text = sanitize_str('What drives you?')
        self.assertEqual(text, 'what_drives_you_')

    def test_credential_path(self):
        path = get_google_credential_path()
        self.assertEqual(type(path), str)
        self.assertGreater(len(path), 0)

    def test_drive_service(self):
        service = get_service()
        self.assertEqual(type(service), Resource)
        
    def test_credentials(self):
        credentials = get_google_credentials()
        credentials.get_access_token()
        self.assertFalse(credentials.access_token_expired)

    def test_time_node(self):
        dt = datetime.datetime.now()
        node = get_time_node(dt)
        self.assertEqual(dt.minute, node.properties['value'])


if __name__ == '__main__':
    unittest.main()