# -*- coding: utf-8 -*-
# @author: franky
# @email: runping@shanshu.ai
# @date: 2018/08/02

import configparser
import os


PROJECT_DIR = os.path.dirname(os.path.abspath(__file__)) + '/..'


def preprocess_param(param):
    dict_param = {}
    for key, value in param:
        if value.isdigit():
            dict_param[key] = int(value)
        else:
            dict_param[key] = value
    return dict_param


class PostgresqlConfig:
    def __init__(self):
        config = configparser.ConfigParser()
        dir_cfg = PROJECT_DIR + '/config/cfg/postgresql.cfg'
        with open(dir_cfg, 'r') as f:
            config.read_file(f)
        param = config.items('postgresql_user_info')

        self.param = preprocess_param(param)


class LivyConfig:
    def __init__(self):
        config  = configparser.ConfigParser()
        dir_cfg = PROJECT_DIR + '/config/cfg/livy.cfg'
        with open(dir_cfg, 'r') as f:
            config.read_file(f)
        param = config.items('livy_info')
        self.param = preprocess_param(param)


class MysqlConfig:
    def __init__(self):
        config  = configparser.ConfigParser()
        dir_cfg = PROJECT_DIR + '/config/cfg/mysql.cfg'
        with open(dir_cfg, 'r') as f:
            config.read_file(f)
        param = config.items('mysql_info')
        self.param = preprocess_param(param)
