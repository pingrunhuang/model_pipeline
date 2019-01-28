from sqlalchemy import Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class ExceptionTable(Base):
    __tablename__ = "exception"

    id = Column(Integer, primary_key=True, autoincrement=True)
    step_name = Column(String)
    reason = Column(String)

    def __repr__(self):
        return str([self.id, self.step_name, self.reason])


class StepTable(Base):
    __tablename__ = "step"

    id = Column(Integer, primary_key=True, autoincrement=True)
    step_id = Column(String)
    status = Column(Integer)
    graph_id = Column(Integer)

    def __repr__(self):
        return str([self.id, self.step_id, self.status, self.graph_id])


class GraphTable(Base):
    __tablename__ = "graph"

    id = Column(Integer, primary_key=True, autoincrement=True)
    status = Column(Integer)

    def __repr__(self):
        return str([self.id, self.status])

