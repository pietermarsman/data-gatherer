import luigi

from config import settings
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


if __name__ == "__main__":
    luigi.run()