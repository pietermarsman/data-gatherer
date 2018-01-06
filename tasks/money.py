import json
import os
from datetime import datetime

import luigi
import pandas as pd
from dateutil import parser
from neomodel import StructuredNode

from action import BankTransferAction
from google_drive import DATA_DIR, get_time_node
from intangible import BankAccount


class BankMutation(luigi.ExternalTask):
    original_file_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.original_file_path)


class ParseBankMutation(luigi.Task):
    COL = ['Rekeningnummer', 'Muntsoort', 'Transactiedatum', 'Beginsaldo', 'Eindsaldo', 'Rentedatum',
           'Transactiebedrag', 'Omschrijving']
    original_file_path = luigi.Parameter()

    def requires(self):
        return [BankMutation(self.original_file_path)]

    def output(self):
        file_name = "%.4d%.2d.json" % (datetime.now().year, datetime.now().month)
        dir = os.path.join(DATA_DIR, "ParseBankMutation", file_name)
        return luigi.LocalTarget(dir)

    def run(self):
        df = pd.read_csv(self.input()[0].path, sep='\t', encoding='ISO-8859-1', names=self.COL, dtype=str)
        records = df.to_dict(orient='records')
        for record in records:
            record['Beginsaldo'] = float(record['Beginsaldo'].replace(',', '.'))
            record['Eindsaldo'] = float(record['Eindsaldo'].replace(',', '.'))
            record['Rentedatum'] = datetime.strptime(record["Rentedatum"], "%Y%m%d").isoformat()
            record['Transactiebedrag'] = float(record['Transactiebedrag'].replace(',', '.'))
            record['Transactiedatum'] = datetime.strptime(record["Transactiedatum"], "%Y%m%d").isoformat()

        with self.output().open('w') as f:
            json.dump(records, f, indent=4, sort_keys=True)


class LoadBankMutation(luigi.Task):
    original_file_path = luigi.Parameter()

    def requires(self):
        return [ParseBankMutation(self.original_file_path)]

    def output(self):
        file_name = "%.4d%.2d.log" % (datetime.now().year, datetime.now().month)
        dir = os.path.join(DATA_DIR, "LoadBankMutation", file_name)
        return luigi.LocalTarget(dir)

    def run(self):
        with self.input()[0].open() as f:
            records = json.load(f)

        for record in records:
            raw_dt_node = get_time_node(parser.parse(record['Transactiedatum']), 'Day')
            dt_node = StructuredNode.inflate(raw_dt_node)

            omschrijving = "%s (%s)" % (record['Omschrijving'], record["Transactiedatum"])

            account = BankAccount.get_or_create({'name': record['Rekeningnummer']})[0]
            action = BankTransferAction.get_or_create({
                'name': omschrijving,
                'price': record["Transactiebedrag"]
            })[0]
            action.account.connect(account)
            action.datetime.connect(dt_node)

        with self.output().open('w') as f:
            f.write('Wrote %d records to graph' % len(records))


class LoadAllBankMutations(luigi.Task):
    dir = luigi.Parameter()

    def requires(self):
        files = os.listdir(str(self.dir))
        files = [f for f in files if f.endswith('.txt')]
        files = [os.path.join(str(self.dir), f) for f in files]
        tasks = [LoadBankMutation(f) for f in files]
        return tasks

    def output(self):
        file_name = "%.4d%.2d.log" % (datetime.now().year, datetime.now().month)
        dir = os.path.join(DATA_DIR, "LoadAllBankMutations", file_name)
        return luigi.LocalTarget(dir)


if __name__ == "__main__":
    luigi.run()
