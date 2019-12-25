import csv
import os

from django.db import IntegrityError

from basimilch.management.commands.import_data import Command as CustomCommand


class Command(CustomCommand):
    """
    Basecommand to insert data into the database from a csv file.
    To execute it, run python manage.py import_data path1 [path2, ...]
    The filename needs to be called table_name.csv, where table_name
    refers to a table either in Juntagrico or in Juntagrico_Custom_Sub
    """

    @staticmethod
    def create_update_tuples(row):
        rv = []
        for key, value in row.items():
            rv.append({key: value})
        return rv

    def handle(self, *args, **options):
        for f in options['files']:
            table_name = os.path.basename(f).split('.')[0]
            table_name = table_name.replace('Update', '')
            table = self.name_to_model(table_name)
            with open(f, newline='', encoding="utf-8-sig") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    row = self.resolve_foreign_keys(row)
                    row = self.create_update_tuples(row)
                    try:
                        table.objects.filter(**row[0]).update(**row[1])
                    except IntegrityError:
                        print(f'{row} already in DB')
