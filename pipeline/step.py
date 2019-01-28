from pipeline.status import Status
from pipeline.exception import TypeMissMatchException, PipeLineException
from pipeline.connector_factory import ConnectorFactory
from pipeline.sys_tables import ExceptionTable, StepTable


class Step:

    __slots__ = ["id", "next", "status", "prev", "out", "processor", "connector", "graph_id", "table"]

    def __init__(self, graph_id, unique_id, conf, db_type='mysql'):
        self.connector = ConnectorFactory(db_type)
        self.id = unique_id
        self.graph_id = graph_id
        # 0:failed, 1:not started, 2:computing, 3:finished
        self.status = Status.not_started
        self.processor = conf["processor"]
        self.next = None
        self.prev = []
        # processed data
        self.out = None
        self.table = None

    def set_next(self, step):
        self.next = step

    def add_prev(self, prev):
        # TODO: don't check for now, use decorator pattern in the future
        # if not issubclass(type(prev), Step):
        #     raise TypeMissMatchException("{} is not type of Step".format(type(prev)))
        self.prev.append(prev)

    def save(self):
        """
        if saved, the result of this node will be saved to disk, else a python object will be returned
        :return:
        """
        if self.out:
            print("Save the output of this step to the external storage system")

    def __eq__(self, other):
        return other.id == self.id

    def __repr__(self):
        return self.id

    def init_step_status(self):
        """
        初始化数据库里面的步骤状态
        :return:
        """
        with self.connector.init_session() as sess:
            result = sess.add(StepTable(step_id=self.id, status=self.status, graph_id=self.graph_id))
            sess.commit()
        print("Step {} initialized {}".format(self.id, result))

    def update_step_status(self, new_status):
        if new_status not in [0, 1, 2, 3]:
            raise TypeMissMatchException("Step status could only be 0,1,2,3")

        self.status = new_status
        with self.connector.init_session() as sess:
            sess.query(StepTable).filter(StepTable.step_id == self.id).update({'status': str(new_status)})
            sess.commit()

    def run(self, *args, **kwargs):
        """
        :param save: if to save the result or not
        :param data: data to be saved
        :return: boolean if the task is success or not
        """
        print(self.id, "'s prev nodes: " + ",".join(["step_id:{}-status:{}".format(x.id, str(x.status)) for x in self.prev]))

        if kwargs.get("save"):
            self.save()
        self.status = Status.finished
        self.table = kwargs.get("table")
        return self.id, self.status

    def get_prev_out(self, prev_processor):
        for prev in self.prev:
            if prev.processor == prev_processor:
                return prev.out
        raise PipeLineException("Could not find the parent step {}".format(prev_processor))

    def handle_exception(self, msg):
        self.connector.bulk_insert_mappings(ExceptionTable, [dict(step_name=self.id, reason=str(msg))])
        self.status = Status.failed


if __name__ == "__main__":
    pass
