import configparser

from sqlalchemy import create_engine
from sqlalchemy.sql import text


class Database:
    def __init__(self, config_path: str = 'mysql.ini'):
        config = configparser.ConfigParser()
        config.read(config_path)

        self.host = config['Database']['host']
        self.port = config['Database']['port']
        self.database = config['Database']['database']
        self.user = config['Database']['user']
        self.password = config['Database']['password']
        self.table = config['Database']['table']

    def connect(self, initial_connection: bool = False):
        if initial_connection:
            engine = create_engine(f'mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}')
            with engine.connect() as connection:
                statement = text(f"DROP DATABASE IF EXISTS {self.database}")
                connection.execute(statement)

                statement = text(f"CREATE DATABASE {self.database}")
                connection.execute(statement)

        engine = create_engine(f'mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}')
        self.engine = engine
