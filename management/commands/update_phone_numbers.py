from .import_data import Command as CustomCommand
from juntagrico import entity as je


class Command(CustomCommand):
    """
    Management command to update phone numbers in db
    (from rails db)
    """

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            '--table_name',
        )

    @staticmethod
    def add_notes(member, phone_type, number):
        if member.notes:
            member.notes = member.notes + f"\n{phone_type}: {number}"
        else:
            member.notes = f"{phone_type}: {number}"

    @staticmethod
    def same_number(number_1, number_2):
        number_1_clean = number_1.replace(" ", "")[-9:-1]
        number_2_clean = number_2.replace(" ", "")[-9:-1]
        return number_1_clean == number_2_clean

    def put_in_db(self, table, row):
        try:
            mem = je.member.Member.objects.get(email=row['email'])
        except je.member.Member.DoesNotExist:
            print(f"{row['email']} not in database for some reason")
        else:
            tel_home = row['tel_home']
            tel_mobile = row['tel_mobile']
            tel_office = row['tel_office']

            if mem.phone == 'NULL':
                mem.phone = None
            if mem.mobile_phone == 'NULL':
                mem.mobile_phone = None

            if tel_home != 'NULL':
                if not mem.phone:
                    mem.phone = tel_home
                    print(f"put phone {tel_home}")
                elif not self.same_number(mem.phone, tel_home):
                    print(f"differente home number in db and csv {tel_home}")
                    self.add_notes(mem, "Tel home alte DB", tel_home)

            if tel_mobile != 'NULL':
                if not mem.mobile_phone:
                    mem.mobile_phone = tel_mobile
                    print(f"put mobile {tel_mobile}")
                elif not self.same_number(mem.mobile_phone, tel_mobile):
                    print(f"differente mobile number in db and csv {tel_mobile}")
                    self.add_notes(mem, "Tel mobile alte DB", tel_mobile)

            if tel_office != 'NULL':
                self.add_notes(mem, "Tel office alte DB", tel_office)
            mem.save()
