from typing import Optional

from fastapi import FastAPI, HTTPException, Query
import pandas as pd
from sqlalchemy.sql import text

from database import Database
from web_scraper import WebScraper
from logger import Logger


app = FastAPI()

db = Database()
db.connect()
logger = Logger('api')


@app.on_event("startup")
def startup():
    logger.info('Checking up database')
    statement = text(f"select count(*) from {db.database}.{db.table};")
    try:
        res = db.connection.execute(statement)
        if res.scalar() > 0:
            data_exists = True
            logger.info('Database check - OK')
    except Exception as err:
        data_exists = False
    if not data_exists:
        logger.warning('Database check - BAD')
        logger.info('Filling database with data')
        ws = WebScraper(database=db)
        ws.scrape()


@app.get("/db/")
def test(manufacturer: Optional[str] = Query(None, description="Manufacturer name to query", example="TestTest"),
         category: Optional[str] = Query(None, description="Category name to query", example="TestTest"),
         model: Optional[str] = Query(None, description="Model name to query", example="TestTest"),
         part_category: Optional[str] = Query(None, description="Part category name to query", example="TestTest"),
         part_number: Optional[str] = Query(None, description="Part number name to query", example="TestTest")):
    sql_script = "select * from manufacturers.manufacturers_table"
    filtering = []
    if manufacturer:
        filtering.append(f'manufacturer = \'{manufacturer}\'')
    if category:
        filtering.append(f'catogory = \'{category}\'')
    if model:
        filtering.append(f'model = \'{model}\'')
    if part_category:
        filtering.append(f'part_category = \'{part_category}\'')
    if part_number:
        filtering.append(f'part_number = \'{part_number}\'')

    if filtering != []:
        filtering = '\' AND \''.join(filtering)
        sql_script = sql_script + ' WHERE ' + filtering
    sql_script = sql_script + ' LIMIT 5' + ';'
    # sql_script = sql_script + ';'

    try:
        query_result = pd.read_sql(sql_script, con=db.engine)
    except Exception as err:
        HTTPException(status_code=404, detail=err)

    return query_result
