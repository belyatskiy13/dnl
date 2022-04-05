import requests

from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

from database import Database


class WebScraper:
    def __init__(self):
        self.URL = 'https://www.urparts.com'
        self.url_header = 'index.cfm/page/catalogue'
        self.database = Database()
        self.database.connect(initial_connection=True)

    @staticmethod
    def comprehension_catch(item):
        part_number = item.contents[0].split(' - ')[0].strip()

        try:
            part_category = item.find('span').contents[0]
        except AttributeError:
            part_category = None

        return part_number, part_category

    def get_soup(self, url_header):
        url = f'{self.URL}/{url_header}'
        page = requests.get(url)
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
        for manufacturer in tqdm(manufacturers[1:3]):
            try:
                soup = self.get_soup(manufacturer[1])
                soup = soup.find('div', class_='c_container allmakes allcategories')
                categories = [[item.contents[0].strip(), item['href']] for item in soup.find_all('a')]
            except AttributeError:
                manufacturer[1] = manufacturer[1].replace(' ', '%20')
                print(f'URL - {self.URL}/{manufacturer[1]} - is broken')

            all_models = pd.DataFrame()
            for category in categories:
                try:
                    soup = self.get_soup(category[1])
                    soup = soup.find('div', class_='c_container allmodels')
                    models = [[item.contents[0].strip(), item['href']] for item in soup.find_all('a')]
                except AttributeError:
                    category[1] = category[1].replace(' ', '%20')
                    print(f'URL - {self.URL}/{category[1]} - is broken')

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
                        print(f'URL - {self.URL}/{model[1]} - is broken')

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
                print(f'{processed_rows} rows processed')
            except ConnectionError as err:
                num_attempts -= 1
                print(f'Connection failed, attempts left: {num_attempts}')
                if num_attempts == 0:
                    raise err


if __name__ == '__main__':
    WebScraper().scrape()
