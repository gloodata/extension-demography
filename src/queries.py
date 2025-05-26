import duckdb
from typing import Dict, List, Tuple


class DuckStore:
    def __init__(self):
        self.conn = None

    def setup(self):
        pass

    def query_one(self, query: str, params: dict, as_dict=True) -> Dict | Tuple | None:
        cursor = self.conn.sql(query, params=params)
        result = cursor.fetchone()

        if result:
            if as_dict:
                return {key: value for key, value in zip(cursor.columns, result)}

            return result

        return None

    def query_all(
        self, query: str, params: dict, as_dict=True
    ) -> List[Dict] | List[Tuple]:
        cursor = self.conn.sql(query, params=params)
        result = cursor.fetchall()

        if as_dict:
            cols = cursor.columns
            return [{key: value for key, value in zip(cols, row)} for row in result]

        return result

    def close(self):
        """Close the database connection."""
        self.conn.close()

    def __enter__(self):
        self.setup()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class DemographyStore(DuckStore):
    def setup(self):
        self.conn = init_db()

    def all_countries(self, as_dict=True):
        return self.query_all(COUNTRIES_ALL, as_dict=as_dict)

    def all_countries_as_kvs(self):
        return [(r["alpha_3"], r["name"]) for r in self.all_countries()]


DEM_MIN_YEAR = 1950
DEM_MAX_YEAR = 2023

DEM_TYPE_TOTAL = "t"
DEM_TYPE_MALE = "m"
DEM_TYPE_FEMALE = "f"

DEM_COLS = [
    "code",
    "year",
    "type",
    "years_0_4",
    "years_5_14",
    "years_15_24",
    "years_25_64",
    "years_65_plus",
]
DEM_COLS_STR = ", ".join(DEM_COLS)


DEM_DATA_COLS_AND_LABELS = [
    ("years_0_4", "0-4"),
    ("years_5_14", "5-14"),
    ("years_15_24", "15-24"),
    ("years_25_64", "25-64"),
    ("years_65_plus", "65+"),
]

DEM_BY_YEAR = f"SELECT {
    DEM_COLS_STR
} FROM demography WHERE year = $year AND type = $type ORDER BY code"
DEM_BY_CODE = f"SELECT {
    DEM_COLS_STR
} FROM demography WHERE code = $code AND type = $type ORDER BY year"
DEM_BY_CODE_AND_YEAR = f"SELECT {
    DEM_COLS_STR
} FROM demography WHERE code = $code AND year = $year AND type = $type"


def dem_by_year(conn, year, type=DEM_TYPE_TOTAL):
    return query_all(conn, DEM_BY_YEAR, year=year, type=type)


def dem_by_code(conn, code, type=DEM_TYPE_TOTAL):
    return query_all(conn, DEM_BY_CODE, code=code, type=type)


def dem_by_code_and_year(conn, code, year, type=DEM_TYPE_TOTAL):
    return query_one_dict(
        conn, DEM_BY_CODE_AND_YEAR, DEM_COLS, code=code, year=year, type=type
    )


def fert_by_country(conn, country):
    return query_all(conn, FERT_BY_COUNTRY, country=country)


def fert_by_year(conn, year):
    return query_all(conn, FERT_BY_YEAR, year=year)


def fert_col_selector(*cols):
    return make_col_selector(FERT_COLS, cols)


FERT_COLS = ["country", "year", "fertility"]
FERT_COLS_STR = ", ".join(FERT_COLS)

FERT_BY_COUNTRY = (
    f"SELECT {FERT_COLS_STR} FROM fertility WHERE country=$country ORDER BY year"
)
FERT_BY_YEAR = (
    f"SELECT {FERT_COLS_STR} FROM fertility WHERE year=$year ORDER BY country, year"
)
COUNTRY_COLS = [
    "name",
    "alpha_2",
    "alpha_3",
    "code",
    "region",
    "sub_region",
    "region_code",
    "sub_region_code",
]
COUNTRY_COLS_STR = ", ".join(COUNTRY_COLS)
COUNTRIES_ALL = f"SELECT {COUNTRY_COLS_STR} FROM country ORDER BY name"
COUNTRIES_BY_REGION = f"SELECT {
    COUNTRY_COLS_STR
} FROM country WHERE region_code = $region_code ORDER BY name"
COUNTRIES_BY_SUB_REGION = f"SELECT {
    COUNTRY_COLS_STR
} FROM country WHERE sub_region_code = $sub_region_code ORDER BY name"
COUNTRIES_KEY_AND_LABEL_PAIRS = (
    "SELECT alpha_3 AS key, name AS label FROM country ORDER BY label"
)
COUNTRY_BY_FUZZY_NAME = f"""
SELECT {COUNTRY_COLS_STR}
FROM country
WHERE
    name ILIKE $name OR
    alpha_3 ILIKE $name OR
    alpha_2 ILIKE $name
"""
CREATE_TABLE_COUNTRY = """
CREATE TABLE country AS
SELECT
    name,
    "alpha-2" AS alpha_2,
    "alpha-3" AS alpha_3,
    "country-code" AS code,
    region,
    "sub-region" AS sub_region,
    "region-code" AS region_code,
    "sub-region-code" AS sub_region_code
FROM read_csv_auto($file_path)
"""
CREATE_TABLE_DEMOGRAPHY = f"""
CREATE TABLE demography AS
SELECT {DEM_COLS_STR} FROM read_parquet($file_path)
WHERE code != ''
"""
CREATE_TABLE_FERTILITY = """
CREATE TABLE fertility AS
SELECT
    "Code" AS country,
    "Year" AS year,
    "Fertility Rate" AS fertility
FROM read_csv_auto($file_path)
WHERE country != ''
"""


def countries_by_region(conn, region_code):
    return query_all(conn, COUNTRIES_BY_REGION, region_code=region_code)


def countries_by_sub_region(conn, sub_region_code):
    return query_all(conn, COUNTRIES_BY_SUB_REGION, sub_region_code=sub_region_code)


def country_key_and_label_pairs(conn):
    return query_all(conn, COUNTRIES_KEY_AND_LABEL_PAIRS)


def get_country_by_fuzzy_name(conn, name):
    return query_one_dict(conn, COUNTRY_BY_FUZZY_NAME, COUNTRY_COLS, name=name)


def query_all(conn, query, **args):
    return conn.execute(query, args).fetchall()


def query_one(conn, query, **args):
    return conn.execute(query, args).fetchone()


def query_one_dict(conn, query, cols, **args):
    t = query_one(conn, query, **args)
    return dict(zip(cols, t)) if t is not None else None


def make_col_selector(all_cols, cols_to_pick):
    indexes = [all_cols.index(col) for col in cols_to_pick]
    return lambda row: [row[i] for i in indexes]


def init_db(
    dem_file_path="./demography.parquet",
    country_file_path="./countries.csv",
    fertility_file_path="./fertility-rage.csv",
):
    print("loading data from", dem_file_path, country_file_path, fertility_file_path)
    conn = duckdb.connect()

    conn.execute(CREATE_TABLE_DEMOGRAPHY)
    conn.execute(CREATE_TABLE_COUNTRY)
    conn.execute(CREATE_TABLE_FERTILITY)

    return conn


print(f"""
-- name: fert_by_country(country)
{FERT_BY_COUNTRY}

-- name: fert_by_year(year)
{FERT_BY_YEAR}

-- name: countries_all()
{COUNTRIES_ALL}

-- name: countries_by_region(region_code)
{COUNTRIES_BY_REGION}

-- name: countries_by_sub_region(sub_region_code)
{COUNTRIES_BY_SUB_REGION}

-- name: country_key_and_label_pairs()
{COUNTRIES_KEY_AND_LABEL_PAIRS}

-- name: get_country_by_fuzzy_name(name)
{COUNTRY_BY_FUZZY_NAME}

-- name: create_table_country()
{CREATE_TABLE_COUNTRY}

-- name: create_table_fertility()
{CREATE_TABLE_FERTILITY}

-- name: create_table_demography()
{CREATE_TABLE_DEMOGRAPHY}

-- name: dem_by_country_code_year_and_type(country, year, type)
{DEM_BY_CODE_AND_YEAR}

-- name: dem_by_country_code_and_type(country, type)
{DEM_BY_CODE}

-- name: dem_by_year_and_type(year, type)
{DEM_BY_YEAR}
""")
