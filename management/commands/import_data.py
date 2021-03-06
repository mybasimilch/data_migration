import csv
import os
from datetime import datetime

import pytz
from django.core.management.base import BaseCommand
from juntagrico_custom_sub import models as csm

from juntagrico import entity as je


class Command(BaseCommand):
    """
    Basecommand to insert data into the database from a csv file.
    To execute it, run python manage.py import_data path1 [path2, ...]
    The filename needs to be called table_name.csv, where table_name
    refers to a table either in Juntagrico or in Juntagrico_Custom_Sub
    """

    table = None

    juntagrico_tables = {
        "Depot": je.depot.Depot,
        "Member": je.member.Member,
        "Subscription": je.subs.Subscription,
        "SubscriptionType": je.subtypes.SubscriptionType,
        "Share": je.share.Share
    }

    def add_arguments(self, parser):
        parser.add_argument('files', nargs='+', type=str)
        parser.add_argument(
            '--ignore-if-key-exists',
        )
        parser.add_argument(
            '--datetime_keys', nargs='+', type=str
        )

    def name_to_model(self, table_name):
        if table_name in self.juntagrico_tables:
            return self.juntagrico_tables[table_name]
        elif hasattr(csm, table_name):
            return getattr(csm, table_name)
        else:
            raise ValueError(f'{table_name} could not be associated with a model')

    def delete_old_and_insert(self, row, **options):
        if 'update_by' in options:
            # key = options['update_by']
            # dikt = {key: row[key]k}
            # old_entries = self.table.objects.filter(**dikt)
            # old_entries.delete()
            obj = self.table.objects.create(**row)
            obj.save()
        else:
            self.table.objects.update_or_create(**row)

    @staticmethod
    def parse_time(row, keys):
        rv = {**row}
        for k in keys:
            if rv[k] != 'NULL':
                time = datetime.strptime(rv[k][0:10], '%Y-%m-%d')
                time = pytz.timezone('Europe/Zurich').localize(time)
            else:
                time = None
            rv[k] = time
        return rv

    @staticmethod
    def ignore_existing(table, row, key):
        rv = table.objects.filter(**{key: row[key]})
        if not len(rv):
            return row

    @staticmethod
    def clean_2019(row):
        rv = {**row}
        if 'member_2019' in rv:
            rv.pop('member_2019')
        return rv

    def resolve_foreign_keys(self, row):
        rv = {}
        for cell in row.items():
            if cell[1] == '':  # clean empty entries
                continue
            elif "[" in cell[0]:
                column, fk_relationship = cell[0].split("[")
                fk_table_name, fk_column = fk_relationship.split(".")
                fk_column = fk_column.strip("]")
                fk_table = self.name_to_model(fk_table_name)
                try:
                    related_object = fk_table.objects.get(**{fk_column: cell[1]})
                except fk_table.DoesNotExist:
                    raise ValueError(f'{cell[1]} not valid for {fk_column} in {fk_table}')
                except fk_table.MultipleObjectsReturned:
                    raise ValueError(f'{cell[1]} returned multiple items for {fk_column} in {fk_table}')
                except Exception:
                    raise ValueError(f'Problem with {fk_column} in {fk_table}, value is {cell[1]}')
                rv[column] = related_object
            else:
                rv[cell[0]] = cell[1]
        return rv

    @staticmethod
    def put_in_db(table, row):
        table.objects.update_or_create(**row)

    def handle(self, *args, **options):
        for f in options['files']:
            if options['table_name']:
                table_name = options['table_name']
            else:
                table_name = os.path.basename(f).split('.')[0]
            table = self.name_to_model(table_name)
            with open(f, newline='', encoding="utf-8-sig") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # row = self.clean_2019(row)
                    row = self.resolve_foreign_keys(row)
                    if options['ignore_if_key_exists']:
                        key = options['ignore_if_key_exists']
                        row = self.ignore_existing(table, row, key)
                        if not row:
                            continue
                    if options['datetime_keys']:
                        keys = options['datetime_keys']
                        row = self.parse_time(row, keys)
                    try:
                        self.put_in_db(table, row)
                    except Exception as e:
                        print(f'{row} exception: {e}')
