from constructs import Construct
from stacks.snow_stack import Root, SnowStack


class WarehouseStack(SnowStack):
    def deploy(self):
        """Deploy all warehouses defined in config"""
        for env in self.configs['environments']:
            for db, conf in self.configs['warehouses'].get('warehouses', []).items():
                for size in conf['sizes']:
                    self.create_warehouse(db, size, env)
