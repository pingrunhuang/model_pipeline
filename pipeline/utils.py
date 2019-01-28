from requests import get, post, delete
from pipeline.configuration import LivyConfig
import logging


class LivyCaller:
    def __init__(self):
        config = LivyConfig()
        self.logger = logging.getLogger(type(self).__name__)
        self.LIVY_SERVER = config.param.get('livy_server')
        self.BATCH_PAYLOAD = {
            "file": config.param.get('jar_path'),
            "jars": [],
            "files": [],
            "className": config.param.get('classname'),
            "name": config.param.get('appname'),
            "driverCores": config.param.get('driver_class'),
            "driverMemory": config.param.get('driver_memory'),
            "executorCores": config.param.get('executor_cores'),
            "executorMemory": config.param.get('executor_memory'),
            "numExecutors": config.param.get('num_executors'),
            "conf": {
                "spark.executor.cores": 2,
                "spark.cores.max": 4,
                "master": "yarn",
                "livy.spark.master": "yarn-client"
            },
            "args": []
        }

    def getBatches(self, start_index, num_of_sessions):
        """
        Returns all the active batch sessions
        """
        url = "{}/batches".format(self.LIVY_SERVER)
        payload = {"from": start_index, "size": num_of_sessions}
        result = post(url, json=payload)
        if result.ok:
            self.logger.debug(result.text)
        else:
            self.logger.error(result.text)

    def createBatch(self, args, jars=[]):
        """
        Run a batch process
        """
        self.BATCH_PAYLOAD["args"] = args
        self.BATCH_PAYLOAD["jars"] = jars
        url = "{}/batches".format(self.LIVY_SERVER)
        self.logger.debug("Creating batch with {}".format(self.BATCH_PAYLOAD))
        result = post(url, json=self.BATCH_PAYLOAD)
        if result.ok:
            self.logger.info("Successfully created a batch")
            self.logger.debug(result.text)
        else:
            self.logger.error("Creating batch failed {}".format(result.text))
        return result

    def getBatch(self, batchId):
        """
        Return batch session info
        """
        url = "{}/batches/{}".format(self.LIVY_SERVER, batchId)
        result = get(url)
        if result.ok:
            self.logger.info(result.text)

    def killBatch(self, batchId):
        """
        kills the batch job
        """
        url = "{}/batches/{}".format(self.LIVY_SERVER, batchId)
        result = delete(url)
        if result.ok:
            self.logger.info("Batch {} deleted".format(batchId))

    def getBatchLog(self, batchId, offset, lines):
        """
        Gets the log lines from this batch.
        @param Offset int
        @param lines Max number of log lines to return	int
        """
        url = "{}/batches/{}/log".format(self.LIVY_SERVER, batchId)
        params = {"from": offset, "size": lines}
        result = get(url, params=params)
        if result.ok:
            self.logger.info(result.text)


if __name__ == "__main__":
    pass