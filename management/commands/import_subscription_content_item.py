import csv
from .import_data import Command as CustomCommand
from juntagrico_custom_sub import models as csm
from django.db import IntegrityError


class Command(CustomCommand):
    table = csm.SubscriptionContentItem

    products = {
        'Rohmilch': csm.Product.objects.get(name='Rohmilch'),
        'Naturejoghurt': csm.Product.objects.get(name='Naturejoghurt'),
        'Fruchtjoghurt': csm.Product.objects.get(name='Fruchtjoghurt'),
        'Quark': csm.Product.objects.get(name='Quark'),
        'Käse': csm.Product.objects.get(name='Zusatzkäse'),
        'Wochenkäse klein': csm.Product.objects.get(name='Wochenkäse klein'),
        'Wochenkäse gross': csm.Product.objects.get(name='Wochenkäse gross'),
    }

    def determine_product_amount(self, prod, amount):
        if prod == "Käserei fix":
            small = int(amount) % 2
            large = int(amount) // 2  # in juntagrico, a large Wochenkäse counts as 2 units
            if large:
                return self.products['Wochenkäse gross'], large
            if small:
                return self.products['Wochenkäse klein'], small

        else:
            return self.products[prod], amount

    def unpivot_row(self, row):
        sc = row['subscription_content']
        rv = []
        for key, value in row.items():
            if key == 'subscription_content' or not value:
                continue
            else:
                p, a = self.determine_product_amount(key, value)
                rv.append({
                    'subscription_content': sc,
                    'product': p,
                    'amount': a
                })
        return rv

    def handle(self, *args, **options):

        for f in options['files']:
            with open(f, newline='', encoding='utf-8-sig') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    row = self.clean_2019(row)
                    row = self.resolve_foreign_keys(row)
                    rows = self.unpivot_row(row)
                    for r in rows:
                        try:
                            self.delete_old_and_insert(r, **options)
                        except IntegrityError:
                            print(f'{r} already in DB')
