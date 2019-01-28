from pipeline.exception import DAGBaseException
from pipeline.connector_factory import ConnectorFactory
from pipeline.sys_tables import ExceptionTable


class ExceptionHandler:
    def __init__(self, uuid, conn=None):
        self.uuid = uuid
        self.conn = conn if conn else ConnectorFactory("mysql")

    def __call__(self, func, *args, **kwargs):
        def new_func(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except DAGBaseException as e:
                self.conn.bulk_insert_mappings(ExceptionTable, dict(step_name=self.uuid, reason=str(e)))
                print("exception catched")
        return new_func


if __name__ == "__main__":
    pass
