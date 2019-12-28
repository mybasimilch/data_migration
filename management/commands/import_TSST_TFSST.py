import csv
import re

from django.db import IntegrityError

from .import_data import Command as CustomCommand
from juntagrico import models as jm


class Command(CustomCommand):
    """
    Basecommand to insert TSST and TFSST many-to-many tables from a csv file into the database.
    """
    table = jm.TFSST

    def parse_row(self, row):
        rows = []
        subs_types = row["type[SubscriptionType.size__units]"]
        subs_types = [int(s) for s in re.split(r" |\+", subs_types) if s.isdigit()]
        for st in subs_types:
            rows.append(
                {
                    "subscription[Subscription.primary_member__email]": row[
                        "subscription[Subscription.primary_member__email]"
                    ],  # noqa: E501
                    "type[SubscriptionType.size__units]": st,
                }
            )
        return rows

    def insert_rows_in_db(self, rows, **options):
        for r in rows:
            r = self.resolve_foreign_keys(r)
            try:
                self.delete_old_and_insert(r, **options)
            except IntegrityError:
                print(f"{r} already in DB")

    def handle(self, *args, **options):
        for f in options["files"]:
            with open(f, newline="", encoding="utf-8-sig") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    row = self.clean_2019(row)
                    parsed = self.parse_row(row)
                    self.insert_rows_in_db(parsed, **options)
