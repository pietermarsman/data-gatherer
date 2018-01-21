import luigi

from config import settings
from geo import LoadAllGeo
from tasks.browser import LoadAllBrowserHistory
from tasks.fuel import LoadAllFuelDrive
from tasks.money import LoadAllBankMutations
from tasks.todo import LoadAllTodo


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


if __name__ == "__main__":
    luigi.run()
