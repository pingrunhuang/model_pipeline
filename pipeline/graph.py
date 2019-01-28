# -*- coding: utf-8 -*-
import configparser
import os
from pipeline.connector_factory import ConnectorFactory
from pipeline.status import Status
from pipeline.exception import DAGBaseException, PipeLineException
from pipeline.sys_tables import GraphTable
CONFIGURATION_PATH = 'configuration/conf.ini'


class Configuration:

    def __init__(self, conf_path=None):
        self.conf = configparser.ConfigParser()
        if not conf_path:
            self.read()
        else:
            self.read(conf_path)

    def read(self, path=CONFIGURATION_PATH):
        self.conf.read(path, encoding='UTF-8')

    def get_all(self):
        result = {}
        for section, value in self.conf.items():
            if not result.get(section):
                result[section] = {}
            for entry in value:
                result[section].update({entry: value[entry]})
        return result

    def get_all_except_default(self):
        for section in self.conf.sections():
            if not result.get(section):
                result[section] = {}
            for entry in self.conf[section]:
                result[section].update({entry: self.conf[section][entry]})
        return result


class Graph:
    def __init__(self, conf_dict):
        """
        根据前端传入的json数据生成graph，定义step1为头节点
        :param conf_dict:
        :return:
        """
        self.config = conf_dict
        if not os.path.exists("data"):
            os.mkdir("data")
        if not os.path.exists("processors"):
            os.mkdir("processors")
        self.traversed_task = {}
        self.connector_factory = ConnectorFactory("mysql")
        # 用于存储即将处理的任务队列
        self.tasks = []
        self.steps_status = {}

        from processors.poi_encoding.encoding import GaodeEncode, SixLayerDictionary
        from processors.dummy import DummyPrint, Pause
        self.classes = {
            "GaodeEncode": GaodeEncode,
            "SixLayerDictionary": SixLayerDictionary,
            "DummyPrint": DummyPrint,
            "Pause": Pause,
            "": DummyPrint
        }
        # initialize graph status
        self.connector_factory.bulk_insert_mappings(GraphTable, [dict(status=Status.not_started)])
        self.id, self.status = self.query_latest_graph()

        for root in map(lambda x: self.classes[conf_dict.get(x)["processor"]](self.id, x, conf_dict.get(x)),
                        [conf for conf in conf_dict if conf_dict[conf]["prev"] == "start"]):
            root = self.build(root)
            self.tasks.append(root)
        self.result_table = None

    def is_executable_step(self, step):
        """
        判断某个步骤是否可以执行，检查每个父节点是否已经执行
        :param step: 某个需要检验的节点
        :return: 是否可执行
        """
        # TODO: don't check for now, use decorator pattern in the future
        # if not issubclass(type(step), Step):
        #     raise TypeMissMatchException("{} is not type of Step".format(type(step)))

        for p in step.prev:
            if p.status == Status.not_started or p.status == Status.computing or p.status == Status.failed:
                return False
        return True

    def query_latest_graph(self):
        with self.connector_factory.init_session() as sess:
            max_graph = sess.query(GraphTable).order_by(GraphTable.id.desc()).first()
            ret = (getattr(max_graph, "id"), getattr(max_graph, "status"))
        return ret

    def update_graph_status(self, new_status):
        """
        This function will update the status of the current graph and also the graph table
        :param new_status:
        :return:
        """

        self.status = new_status
        with self.connector_factory.init_session() as sess:
            sess.query(GraphTable).filter(GraphTable.id == self.id).update({"status": new_status})
            sess.commit()

    def build(self, root):
        """
        given a root, traverse deep first search and update the task map
        :param root: root step
        :return: root step
        """
        # TODO: don't check for now, use decorator pattern in the future
        # if not issubclass(type(root), Step):
        #     raise TypeMissMatchException("{} is not type of Step".format(type(root)))

        cur_step = root
        # if current step is in the config map
        while self.config.get(cur_step.id):
            cur_step.init_step_status()
            self.steps_status.update({cur_step.id: cur_step.status})
            next_id = self.config[cur_step.id]["next"]
            if self.config[next_id]["next"] != "end":
                next_step_conf = self.config[next_id]
                StepType = self.classes[next_step_conf['processor']]
                next_step = StepType(self.id, next_id, next_step_conf) if not self.traversed_task.get(next_id) \
                    else self.traversed_task.get(next_id)
                next_step.add_prev(cur_step)
                cur_step.set_next(next_step)
                self.traversed_task.update({cur_step.id: cur_step})
                cur_step = next_step
            else:
                self.traversed_task.update({cur_step.id: cur_step})
                break
        print(self.traversed_task)
        return root

    @property
    def get_steps_status(self):
        """
        get each step's status inside this graph
        :return:
        """
        return self.steps_status

    def sample(self, limit=100):
        sample_result = []
        if self.result_table:
            with self.connector_factory.init_session() as sess:
                query = sess.query(self.result_table).limit(limit)
                for row in query:
                    sample_result.append(str(row))
            return sample_result
        else:
            raise PipeLineException("No data written into database table")

    def start(self, run_or_show="run"):
        ret = None
        self.update_graph_status(Status.computing)
        while self.tasks:
            cur_task = self.tasks.pop(0)
            # 当前步骤有子节点 and 子节点在task队列里面不存在了
            if cur_task.next and cur_task.next not in self.tasks:
                self.tasks.append(cur_task.next)
            if self.is_executable_step(cur_task):
                cur_task.status = Status.computing
                if run_or_show == "run":

                    try:
                        cur_task.run(**self.config.get(cur_task.id).get("args"))
                        self.steps_status.update({cur_task.id: cur_task.status})

                    except DAGBaseException as e:
                        cur_task.handle_exception(e)
                        self.update_graph_status(Status.failed)
                        self.steps_status.update({cur_task.id: cur_task.status})
                        break
                    except Exception as e:
                        print("Unhandled exception!")
                        cur_task.handle_exception(e)
                        self.update_graph_status(Status.failed)
                        self.steps_status.update({cur_task.id: cur_task.status})
                        break

                    if len(self.tasks) == 0:
                        ret = cur_task
                        self.result_table = cur_task.table

                elif run_or_show == "show":
                    print("Showing: ", cur_task.id, type(cur_task))
                    cur_task.update_step_status(Status.finished)
            elif cur_task not in self.tasks:
                self.tasks.append(cur_task)
        self.update_graph_status(Status.finished)

        return ret


if __name__ == "__main__":
    config = {
        "step1": {
            "prev": ["start"],
            "next": "step2",
            "processor": "DummyPrint",
            "args": {"msg": "step1 dummy print"}
        },
        "step2": {
            "prev": ["step1"],
            "next": "step3",
            "processor": "DummyPrint",
            "args": {"msg": "step2 dummy print"}
        },
        "step3": {
            "prev": ["step2"],
            "next": "step4",
            "processor": "Pause",
            "args": {"time": 10}
        },
        "step4": {
            "prev": ["step3"],
            "next": "step6",
            "processor": "SixLayerDictionary",
            "args": {"province": "广东省", "city": "深圳市", "district": ""}
        },
        "step5": {
            "prev": ["start"],
            "next": "step6",
            "processor": "DummyPrint",
            "args": {"msg": "step5 dummy print"}
        },
        "step6": {
            "prev": ["step4", "step5"],
            "next": "step7",
            "processor": "GaodeEncode",
            "args": {"province": "广东省", "city": "深圳市", "district": ""}
        },
        "step7": {
            "prev": ["step6"],
            "next": "end",
            "processor": "",
            "args": []
        }
    }
    g = Graph(config)
    result = g.start(run_or_show="show")
