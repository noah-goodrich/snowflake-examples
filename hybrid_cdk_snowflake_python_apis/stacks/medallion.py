from .snow import SnowStack


class Medallion(SnowStack):
    def deploy(self):
        medals = [
            ('BRONZE', 'Ingestion'),
            ('SILVER', 'Transformation'),
            ('GOLD', 'Analytics'),
            ('PLATINUM', 'ML')
        ]

        for db, comment in medals:
            self.create_if_not_exists_database(
                db, comment)

        for db, _ in medals:
            sizes = ['xs', 's', 'm', 'l', 'xl', '2xl', '3xl']
            for size in sizes:
                self.create_or_alter_warehouse(db, size)
