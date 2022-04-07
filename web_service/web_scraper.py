import requests
import time

from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy.sql import text

from database import Database
from logger import Logger


class WebScraper:
    def __init__(self, database: Database):
        self.logger = Logger('web-scraper')
        self.URL = 'https://www.urparts.com'
        self.url_header = 'index.cfm/page/catalogue'
        self.database = database

    @staticmethod
    def comprehension_catch(item):
        part_number = item.contents[0].split(' - ')[0].strip()

        try:
            part_category = item.find('span').contents[0]
        except AttributeError:
            part_category = None

        return part_number, part_category

    def get_soup(self, url_header: str, max_retries: int = 5):
        url = f'{self.URL}/{url_header}'
        while max_retries > 0:
            try:
                page = requests.get(url)
                max_retries = 0
            except requests.exceptions.SSLError as err:
                max_retries -= 1
                if max_retries == 0:
                    self.logger.error(f'Connection failed - {url_header}')
                    self.logger.error(f'Dropping database')
                    with self.database.engine.connect() as connection:
                        statement = text(f"DROP DATABASE IF EXISTS {self.database.database}")
                        connection.execute(statement)
                    raise err
                self.logger.warning(f'Connection error {url_header} ... Attempts left {max_retries}')
                time.sleep(10)

        soup = BeautifulSoup(page.content, 'html.parser')
        soup = soup.find('div', id='wrap')
        soup = soup.find('div', id='wrap2')
        soup = soup.find('div', id='contentWrapWide')
        return soup

    def scrape_site(self):
        soup = self.get_soup(self.url_header)
        soup = soup.find('div', class_='c_container allmakes')
        manufacturers = [[item.contents[0].strip(), item['href']] for item in soup.find_all('a')]

        processed_rows = 0
        for manufacturer in manufacturers:
            self.logger.info(f'Scraping {manufacturer}')
            try:
                soup = self.get_soup(manufacturer[1])
                soup = soup.find('div', class_='c_container allmakes allcategories')
                categories = [[item.contents[0].strip(), item['href']] for item in soup.find_all('a')]
            except AttributeError:
                manufacturer[1] = manufacturer[1].replace(' ', '%20')
                self.logger.warning(f'URL - {self.URL}/{manufacturer[1]} - is broken')

            all_models = pd.DataFrame()
            for category in categories:
                try:
                    soup = self.get_soup(category[1])
                    soup = soup.find('div', class_='c_container allmodels')
                    models = [[item.contents[0].strip(), item['href']] for item in soup.find_all('a')]
                except AttributeError:
                    category[1] = category[1].replace(' ', '%20')
                    self.logger.warning(f'URL - {self.URL}/{category[1]} - is broken')

                all_parts = pd.DataFrame()
                for model in models:
                    try:
                        soup = self.get_soup(model[1])
                        soup = soup.find('div', class_='c_container allparts')
                        soup = soup.find('div', class_='c_container allparts')
                        parts = [self.comprehension_catch(item) for item in soup.find_all('a')]
                        parts = pd.DataFrame(columns=['part_number', 'part_category'], data=parts)

                        parts['model'] = model[0]
                        all_parts = pd.concat([all_parts, parts])
                    except AttributeError:
                        model[1] = model[1].replace(' ', '%20')
                        self.logger.warning(f'URL - {self.URL}/{model[1]} - is broken')

                all_parts['category'] = category[0]
                all_models = pd.concat([all_models, all_parts])

            if len(all_models) > 0:
                processed_rows += len(all_models)
                all_models['manufacturer'] = manufacturer[0]
                all_models = all_models[['manufacturer', 'category', 'model', 'part_category', 'part_number']]
                all_models.to_sql(name=self.database.table, con=self.database.engine, if_exists='append', index=False)
        return processed_rows

    def scrape(self, num_attempts=3):
        while num_attempts > 0:
            try:
                processed_rows = self.scrape_site()
                num_attempts = 0
                self.logger.info(f'{processed_rows} rows processed')
            except ConnectionError as err:
                num_attempts -= 1
                self.logger.warning(f'Connection failed, attempts left: {num_attempts}')
                if num_attempts == 0:
                    raise err


if __name__ == '__main__':
    db = Database()
    db.connect()

    ws = WebScraper(database=db)
    ws.scrape()
