import luigi
import os
from neomodel import config as neoconfig

from config import settings
from geo import LoadAllGeo
from tasks.browser import LoadAllBrowserHistory
from tasks.fuel import LoadAllFuelDrive
from tasks.money import LoadAllBankMutations
from tasks.todo import LoadAllTodo
from tomtom import LoadAllTomTomData


class MainTask(luigi.WrapperTask):

    def requires(self):
        yield LoadAllFuelDrive(spreadsheet_id="1JyKdWvl8aHzDU6AjHS2x7Qs03x5DBjm9Ql900WQ5woM",
                               range="Form responses 1!A:D")
        yield LoadAllBankMutations(settings['io']['in'])
        yield LoadAllTodo()
        yield LoadAllBrowserHistory()
        yield LoadAllGeo(
            query='\'{folder}\' in parents and mimeType=\'{mime_type}\' and name=\'{date:%Y%m%d}.geojson\'',
            mime_type='application/vnd.geo+json', folder='1H3nSpN6GePfo8CM0UaIT2do1MDySZqZl')
        yield LoadAllTomTomData(directory=os.path.join(settings['io']['in'], 'tomtom'))


if __name__ == "__main__":
    neoconfig.DATABASE_URL = 'bolt://%s:%s@localhost:7687' % (settings['neo4j']['user'], settings['neo4j']['password'])

    luigi.run()
