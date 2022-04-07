from typing import Optional, List

from fastapi import FastAPI, HTTPException, Query
import pandas as pd
import sqlalchemy
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
    db.create_alcheny_table()


@app.get("/db/")
def query_db(manufacturer: Optional[List[str]] = Query(None,
                                                       description="Manufacturer name to query",
                                                       example="TestTest"),
             category: Optional[List[str]] = Query(None,
                                                   description="Category name to query",
                                                   example="TestTest"),
             model: Optional[List[str]] = Query(None,
                                                description="Model name to query",
             example="TestTest"),
             part_category: Optional[List[str]] = Query(None,
                                                        description="Part category name to query",
                                                        example="TestTest"),
             part_number: Optional[List[str]] = Query(None,
                                                      description="Part number name to query",
                                                      example="TestTest")):
    """
    Get query parameters from host,  query the database and return result.
    returns:
        * Dataframe
    Python example:
        requests.get(url, params={'manufacturer': ['Bomag'], 'category': ['Roller Parts']})
    URL example:
        http://<url>/db/?manufacturer=Bomag&model=BW100&model=BW900
    """
    query = sqlalchemy.select([db.alchemy_table])
    if manufacturer:
        query = query.where(db.alchemy_table.columns.manufacturer.in_(manufacturer))
    if category:
        query = query.where(db.alchemy_table.columns.category.in_(category))
    if model:
        query = query.where(db.alchemy_table.columns.model.in_(model))
    if part_category:
        query = query.where(db.alchemy_table.columns.part_category.in_(part_category))
    if part_number:
        query = query.where(db.alchemy_table.columns.part_number.in_(part_number))

    try:
        ResultProxy = db.connection.execute(query)
        ResultSet = ResultProxy.fetchall()

        df = pd.DataFrame(ResultSet)
        df.columns = ResultSet[0].keys()
        return df
    except Exception as err:
        raise HTTPException(status_code=404, detail=err)
