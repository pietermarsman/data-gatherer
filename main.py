import luigi

from browser import LoadBrowserHistory, TransformBrowserHistory
from google_drive import LoadFuelInGraph
from money import LoadAllBankMutations
from todo import LoadTodoInGraph


class MainTask(luigi.WrapperTask):

    def requires(self):
        return [LoadFuelInGraph("1JyKdWvl8aHzDU6AjHS2x7Qs03x5DBjm9Ql900WQ5woM", "Form responses 1!A:D"),
                LoadAllBankMutations("/Users/pieter/Data/personal/input/abnamro/"),
                LoadTodoInGraph(), TransformBrowserHistory()]


if __name__ == "__main__":
    luigi.run()
