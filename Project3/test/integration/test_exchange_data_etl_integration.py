import csv
import datetime

import psycopg2

from covidmonitor.covidM_data_etl import run
from covidmonitor.utils.db import WarehouseConnection
from covidmonitor.utils.sde_config import get_warehouse_creds


class TestCovidMonitor:
    def teardown_method(self, test_covidM_data_etl_run):
        with WarehouseConnection(
            get_warehouse_creds()
        ).managed_cursor() as curr:
            curr.execute("TRUNCATE TABLE covidM.monitor;")

    def get_covidM_data(self):
        with WarehouseConnection(get_warehouse_creds()).managed_cursor(
            cursor_factory=psycopg2.extras.DictCursor
        ) as curr:
            curr.execute(
                '''SELECT Country,
                        countrycode,
                        slug,
                        newconfirmed,
                        totalconfirmed,
                        newdeaths,
                        totaldeaths,
                        newrecovered,
                        totalrecovered,
                        updateddate
                        FROM covidM.monitor;'''
            )
            table_data = [dict(r) for r in curr.fetchall()]
        return table_data

    def test_covidM_data_etl_run(self, mocker):
        mocker.patch(
            'covidmonitor.covidM_data_etl.get_covidM_data',
            return_value=[
                r
                for r in csv.DictReader(
                    open('test/fixtures/sample_raw_covidM_data.csv')
                )
            ],
        )
        run()
        expected_result = [
            {
                "Country": "ALA Aland Islands",
                "CountryCode": "AX",
                "Slug": "ala-aland-islands",
                "NewConfirmed": 0,
                "TotalConfirmed": 0,
                "NewDeaths": 0,
                "TotalDeaths": 0,
                "NewRecovered": 0,
                "TotalRecovered": 0,
                "UpdatedDate": datetime.datetime(
                    2020, 4, 5, 6, 37, tzinfo=datetime.timezone.utc
                ),
            },
            {
                "Country": "Afghanistan",
                "CountryCode": "AF",
                "Slug": "afghanistan",
                "NewConfirmed": 18,
                "TotalConfirmed": 299,
                "NewDeaths": 1,
                "TotalDeaths": 7,
                "NewRecovered": 0,
                "TotalRecovered": 10,
                "UpdatedDate": datetime.datetime(
                    2020, 4, 5, 6, 37, tzinfo=datetime.timezone.utc
                ),
            },
            {
                "Country": "Albania",
                "CountryCode": "AL",
                "Slug": "albania",
                "NewConfirmed": 29,
                "TotalConfirmed": 333,
                "NewDeaths": 3,
                "TotalDeaths": 20,
                "NewRecovered": 10,
                "TotalRecovered": 99,
                "UpdatedDate": datetime.datetime(
                    2020, 4, 5, 6, 37, tzinfo=datetime.timezone.utc
                ),
            },
            {
                "Country": "Algeria",
                "CountryCode": "DZ",
                "Slug": "algeria",
                "NewConfirmed": 80,
                "TotalConfirmed": 1251,
                "NewDeaths": 25,
                "TotalDeaths": 130,
                "NewRecovered": 28,
                "TotalRecovered": 90,
                "UpdatedDate": datetime.datetime(
                    2020, 4, 5, 6, 37, tzinfo=datetime.timezone.utc
                ),
            },
        ]
        result = self.get_covidM_data()
        assert expected_result == result
