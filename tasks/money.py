import json
import os
from datetime import datetime
from glob import glob

import luigi
import pandas as pd
from dateutil import parser

from config import settings
from misc import get_time_node
from schemas.action import BankTransferAction
from schemas.intangible import BankAccount


class ExtractBankMutation(luigi.ExternalTask):
    original_file_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.original_file_path)


class TransformBankMutation(luigi.Task):
    COL = ['Rekeningnummer', 'Muntsoort', 'Transactiedatum', 'Beginsaldo', 'Eindsaldo', 'Rentedatum',
           'Transactiebedrag', 'Omschrijving']
    original_file_path = luigi.Parameter()

    def requires(self):
        return [ExtractBankMutation(self.original_file_path)]

    def output(self):
        file_name = "%s.json" % os.path.split(self.original_file_path)[-1].split('.')[0]
        dir = os.path.join(settings['io']['out'], TransformBankMutation.__name__, file_name)
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
        return [TransformBankMutation(self.original_file_path)]

    def output(self):
        file_name = "%s.log" % os.path.split(self.original_file_path)[-1].split('.')[0]
        dir = os.path.join(settings['io']['out'], LoadBankMutation.__name__, file_name)
        return luigi.LocalTarget(dir)

    def run(self):
        with self.input()[0].open() as f:
            records = json.load(f)

        for record in records:
            dt_node = get_time_node(parser.parse(record['Transactiedatum']))

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


class LoadAllBankMutations(luigi.WrapperTask):
    dir = luigi.Parameter()

    def requires(self):
        files = glob(os.path.join(self.dir, '*.txt'))
        tasks = [LoadBankMutation(f) for f in files]
        return tasks
