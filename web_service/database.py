import configparser
import time

import sqlalchemy

from logger import Logger


class Database():
    def __init__(self, config_path: str = 'mysql.ini'):
        """
        Database class to connect and work with mysql
        Parameters:
            * config_path - path to database config file
        Attributes:
            * host - host name
            * database - database name
            * user - database user name
            * password - user password to acces database
            * table - database table
        """
        self.logger = Logger('db-connector')
        config = configparser.ConfigParser()
        config.read(config_path)

        self.host = config['Database']['host']
        self.database = config['Database']['database']
        self.user = config['Database']['user']
        self.password = config['Database']['password']
        self.table = config['Database']['table']

    def establish_connection(self):
        """
        Create connection
        Attributes:
            * engine - Connection engine
            * connection - Database connection
        """
        engine = sqlalchemy.create_engine(f'mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.database}')
        self.engine = engine
        self.connection = engine.connect()

    def connect(self, max_retries: int = 5):
        """
        Connect to database
        Parameters:
            * max_retries - max tries to reconnect
        """
        # Reconnection loop, in case mysql container is not started or smth
        while max_retries > 0:
            try:
                self.logger.info('Connecting...')
                self.establish_connection()
                max_retries = 0
                self.logger.info('Connection established')
            except Exception as err:
                max_retries -= 1
                if max_retries == 0:
                    self.logger.error('Connection failed')
                    raise err
                self.logger.warning(f'Connection error... Attempts left {max_retries}')
                time.sleep(10)

    def create_alchemy_table(self):
        """
        Connect to manufacturers table
        """
        metadata = sqlalchemy.MetaData()
        self.alchemy_table = sqlalchemy.Table(self.table, metadata, autoload=True, autoload_with=self.engine)

    def die_and_rise(self):
        """
        In case something went wrong on filling database up.
        The function drops and creates the database again&
        """
        self.logger.error(f'Dropping database')
        statement = sqlalchemy.sql.text(f"DROP DATABASE IF EXISTS {self.database.database}")
        self.connection.execute(statement)
        statement = sqlalchemy.sql.text(f"CREATE DATABASE {self.database.database}")
        self.connection.execute(statement)
