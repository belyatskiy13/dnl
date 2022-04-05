from typing import Optional

from fastapi import FastAPI
import pandas as pd

from database import Database
from web_scraper import WebScraper


app = FastAPI()

WebScraper().scrape()

db = Database()
db.connect()


@app.get('/db/')
def test_func(maunfacturer: Optional[str] = None):
    # pd.read_sql(f'SELECT * from {db.database}.{db.table} where maunfacturer = {maunfacturer};', con=db.engine)
    res = pd.read_sql(f'SELECT * from {db.database}.{db.table} LIMIT 5;', con=db.engine)
    return {"test": res}
