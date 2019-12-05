import csv
import pytz
from datetime import datetime

from basimilch.management.commands.import_data import Command as CustomCommand
from django.db import IntegrityError
from juntagrico import models as jm


class Command(CustomCommand):
    """
    Basecommand to insert assigmenents from a csv file into the database.
    To execute it, run python manage.py import_data path1 [path2, ...]
    The filename needs to be called table_name.csv, where table_name
    refers to a table either in Juntagrico or in Juntagrico_Custom_Sub
    """
    migration_job_type = jm.JobType.objects.get(name='Vergangener Job')

    def parse_row(self, row):
        if row['date'] == '2019':
            return None
        else:
            rv = {** row}
            time = datetime.strptime(row['date'], '%d/%m/%Y')
            time = time.replace(tzinfo=pytz.timezone('Europe/Zurich'))
            recurring_job = jm.RecuringJob.objects.filter(time=time, type=self.migration_job_type).first()
            if not recurring_job:
                recurring_job = jm.RecuringJob.objects.create(time=time, type=self.migration_job_type, slots=0)
            recurring_job.slots += 1
            recurring_job.save()
            rv['job_id'] = recurring_job.id
            rv['amount'] = 1
            del rv['date']
        return rv

    def handle(self, *args, **options):
        for f in options['files']:
            table = jm.Assignment
            with open(f, newline='', encoding="utf-8-sig") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    row = self.parse_row(row)
                    if row:
                        row = self.resolve_foreign_keys(row)
                        try:
                            table.objects.update_or_create(**row)
                        except IntegrityError:
                            print(f'{row} already in DB')
