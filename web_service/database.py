import configparser
import time

from sqlalchemy import create_engine

from logger import Logger


class Database():
    def __init__(self, config_path: str = 'mysql.ini'):
        self.logger = Logger('db-connector')
        config = configparser.ConfigParser()
        config.read(config_path)

        self.host = config['Database']['host']
        self.database = config['Database']['database']
        self.user = config['Database']['user']
        self.password = config['Database']['password']
        self.table = config['Database']['table']

    def establish_connection(self):
        engine = create_engine(f'mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.database}')
        self.engine = engine
        self.connection = engine.connect()

    def connect(self, max_retries: int = 5):
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
