import csv
import os
from django.core.management.base import BaseCommand
from django.db import IntegrityError
from juntagrico import entity as je
from juntagrico_custom_sub import models as csm
from juntagrico.lifecycle import sub
import juntagrico.entity.subs 
from juntagrico.entity import subs


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
        "SubscriptionType": je.subtypes.SubscriptionType
    }

    def add_arguments(self, parser):
        parser.add_argument('files', nargs='+', type=str)
        parser.add_argument(
            '--update_by',
            # action='store_true',
            help='Update? If not specified new entries are appended.',
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

    def handle(self, *args, **options):
        for f in options['files']:
            table_name = os.path.basename(f).split('.')[0]
            table = self.name_to_model(table_name)
            with open(f, newline='', encoding="utf-8-sig") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    row = self.clean_2019(row)
                    row = self.resolve_foreign_keys(row)
                    try:
                        table.objects.update_or_create(**row)
                    except IntegrityError:
                        print(f'{row} already in DB')
