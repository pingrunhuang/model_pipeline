class DAGBaseException(Exception):
    def __init__(self, msg=None):
        super(DAGBaseException, self).__init__(msg)


class ConfigurationTemplateError(DAGBaseException):
    def __init__(self, msg=None):
        super(ConfigurationTemplateError, self).__init__(msg)


class StepNotFoundException(DAGBaseException):
    def __init__(self, msg=None):
        super(StepNotFoundException, self).__init__(msg)


class TypeMissMatchException(DAGBaseException):
    def __init__(self, msg=None):
        super(TypeMissMatchException, self).__init__(msg)


class PipeLineException(DAGBaseException):
    def __init__(self, msg=None):
        super(PipeLineException, self).__init__(msg)


class DBNotSupportedException(DAGBaseException):
    def __init__(self, msg=None):
        super(DBNotSupportedException, self).__init__(msg)
