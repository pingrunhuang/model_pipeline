from pipeline.step import Step
import time


class DummyPrint(Step):
    def __init__(self, graph_id, unique_id, conf):
        super(DummyPrint, self).__init__(graph_id, unique_id, conf)

    def run(self, *args, **kwargs):
        msg = kwargs.get("message")
        print(msg)
        super(DummyPrint, self).run()


class Pause(Step):

    def __init__(self, graph_id, unique_id, conf):
        super(Pause, self).__init__(graph_id, unique_id, conf)

    def run(self, *args, **kwargs):
        duration = int(kwargs.get("time"))
        time.sleep(duration)
        super(Pause, self).run()
