from io import StringIO
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pipeline.exception import DBNotSupportedException, PipeLineException
import psycopg2
import pymysql


class ConnectorFactory:
    def __init__(self, db_type):
        if db_type == "postgre":
            self.db_user = "bdatadev"
            self.db_password = "Shanshu.1204"
            self.db_host = "n301.shanshu"
            self.db_port = 5432
            self.database = "pipeline"
            self.db_url = 'postgresql://{}:{}@{}:{}/{}'.format(self.db_user, self.db_password, self.db_host, str(self.db_port), self.database)
            self.engine = create_engine(self.db_url)
        elif db_type == "mysql":
            self.db_user = "test"
            self.db_password = "test"
            self.db_host = "localhost"
            self.db_port = 3306
            self.database = "data_pipeline"
            self.db_url = 'mysql+pymysql://{}:{}@{}:{}/{}'.format(self.db_user, self.db_password, self.db_host, str(self.db_port), self.database)
            self.engine = create_engine(self.db_url)
        else:
            raise DBNotSupportedException("Currently only postgresql and mysql are supported!")

    @contextmanager
    def init_session(self):
        Session = sessionmaker()
        sess = Session(bind=self.engine)
        yield sess
        sess.close()

    @contextmanager
    def get_connection(self):
        conn = None
        if self.db_url.startswith("postgresql"):
            conn = psycopg2.connect(dbname=self.database, user=self.db_user, password=self.db_password, host=self.db_host, port=self.db_port)
        elif self.db_url.startswith("mysql"):
            conn = pymysql.connect(host=self.db_host, user=self.db_user, password=self.db_password, database=self.database, port=self.db_port)
        if conn:
            yield conn
            conn.close()
        else:
            raise PipeLineException("Connection error!")

    def copy_from(self, table, columns, data, conn, sep='\x1E'):
        if self.db_url.startswith("postgresql"):
            curs = conn.cursor()
            output = StringIO()
            output.write("\n".join(data))
            data.seek(0)
            curs.copy_from(data, table, columns=columns, sep=sep)
            conn.commit()

    def bulk_insert_mappings(self, table, data_dict):
        """

        :param table:
        :param data_dict:
        :return:
        """
        with self.init_session() as sess:
            sess.bulk_insert_mappings(
                table, data_dict
            )
            sess.commit()

