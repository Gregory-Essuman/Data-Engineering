#!/usr/bin/env python3.9

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List
import psycopg2.extras as p
import psycopg2
import logging
import requests
import sys
import os

@dataclass
class DBConnection:
    db: str
    user: str
    password: str
    host: str
    port: int = 5432

class WarehouseConnection:
    def __init__(self, db_conn: DBConnection):
        self.conn_url = (
            f'postgresql://{db_conn.user}:{db_conn.password}@'
            f'{db_conn.host}:{db_conn.port}/{db_conn.db}'
        )

    @contextmanager
    def managed_cursor(self, cursor_factory=None):
        self.conn = psycopg2.connect(self.conn_url)
        self.conn.autocommit = True
        self.curr = self.conn.cursor(cursor_factory=cursor_factory)
        try:
            yield self.curr
        finally:
            self.curr.close()
            self.conn.close()

def get_warehouse_creds() -> DBConnection:
    return DBConnection(
        user=os.getenv('WAREHOUSE_USER', ''),
        password=os.getenv('WAREHOUSE_PASSWORD', ''),
        db=os.getenv('WAREHOUSE_DB', ''),
        host=os.getenv('WAREHOUSE_HOST', ''),
        port=int(os.getenv('WAREHOUSE_PORT', 5432)),
    )

def get_datetime(date_str):
    return datetime.fromisoformat(date_str.replace('Z', '+00:00'))

def get_covidM_data() -> List[Dict[str, Any]]:
    url = "https://api.covid19api.com/summary"
    try:
        r = requests.get(url)
    except requests.ConnectionError as ce:
        logging.error(f"There was an error with the request, {ce}")
        sys.exit(1)
    return r.json().get('Countries', [])

def _get_covidM_insert_query() -> str:
    return '''
    INSERT INTO covidm.monitor (
        country,
        countrycode,
        slug,
        newconfirmed,
        totalconfirmed,
        newdeaths,
        totaldeaths,
        newrecovered,
        totalrecovered,
        updateddate
    )
    VALUES (
        %(Country)s,
        %(CountryCode)s,
        %(Slug)s,
        %(NewConfirmed)s,
        %(TotalConfirmed)s,
        %(NewDeaths)s,
        %(TotalDeaths)s,
        %(NewRecovered)s,
        %(TotalRecovered)s,
        %(UpdatedDate)s
    );
    '''

def run():
    data = get_covidM_data()
    for country in data:
        del country['ID']
        del country['Premium']
        country['UpdatedDate'] = country.pop('Date')
        country['UpdatedDate'] = get_datetime(country.get('UpdatedDate'))
    with WarehouseConnection(get_warehouse_creds()).managed_cursor() as curr:
        p.execute_batch(curr, _get_covidM_insert_query(), data)

if __name__ == "__main__":
    run()
