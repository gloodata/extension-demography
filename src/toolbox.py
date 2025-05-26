from store import DuckStore
from glootil import Toolbox, DynEnum, ContextActionInfo
from enum import Enum


class State(DuckStore):
    async def post_setup(self):
        await self.query("create_table_country", args={"file_path": "countries.csv"})
        await self.query(
            "create_table_demography", args={"file_path": "demography.parquet"}
        )
        await self.query(
            "create_table_fertility", args={"file_path": "fertility-rate.csv"}
        )

    async def all_countries_as_kvs(self):
        return await self.query_to_tuple(
            "country_key_and_label_pairs", key="?", label="?"
        )


tb = Toolbox("gd-demography", "Demography", "Demography Explorer", state=State())


@tb.enum(icon="flag")
class Country(DynEnum):
    @staticmethod
    def load(state: State):
        return state.all_countries_as_kvs()


MAX_YEAR = 2023
DEM_DATA_COLS_AND_LABELS = [
    ("years_0_4", "0-4"),
    ("years_5_14", "5-14"),
    ("years_15_24", "15-24"),
    ("years_25_64", "25-64"),
    ("years_65_plus", "65+"),
]

COUNTRY_TABLE_COLS_INFO = [
    ("name", "Name"),
    ("alpha_2", "ISO Code 2"),
    ("alpha_3", "ISO Code 3"),
    ("code", "Code"),
    ("region", "Region"),
    ("sub_region", "Sub Region"),
    ("region_code", "Region Code"),
    ("sub_region_code", "Sub Region Code"),
]
COUNTRY_TABLE_COL_NAMES = [r[0] for r in COUNTRY_TABLE_COLS_INFO]


@tb.tool(name="Country Table")
async def country_table(state: State):
    """Show a table with information about all countries"""
    rows = await state.query_to_tuple_from_col_names(
        "countries_all", col_names=COUNTRY_TABLE_COL_NAMES
    )
    return {"type": "Table", "columns": COUNTRY_TABLE_COLS_INFO, "rows": rows}


class DemType(Enum):
    TOTAL = "t"
    MALE = "m"
    FEMALE = "f"


@tb.tool(
    name="Demography by Country and Year",
    ui_prefix="Demography for",
    args=dict(country="Country", year="Year"),
    examples=["population pyramid for spain in 2023", "demography for india in 1950"],
)
async def demography_by_country_and_year(state: State, country: Country, year: int):
    """Show a population pyramid for a given country and year"""

    male = await state.query_one(
        "dem_by_country_code_year_and_type",
        args=dict(code=country, year=year, type=DemType.MALE.value),
        default={},
    )
    female = await state.query_one(
        "dem_by_country_code_year_and_type",
        args=dict(code=country, year=year, type=DemType.FEMALE.value),
        default={},
    )

    items = []
    for name, label in reversed(DEM_DATA_COLS_AND_LABELS):
        start = male.get(name, 0)
        end = female.get(name, 0)

        items.append(dict(label=label, start=start, end=end))

    return {"type": "PopulationPyramid", "items": items}


@tb.context_action(tool=demography_by_country_and_year, target=Country)
def demography_by_country_and_year_for_country(ctx: ContextActionInfo):
    return {"args": {"country": ctx.value.get("label"), "year": MAX_YEAR}}


@tb.tool(
    name="World Fertility by Year",
    ui_prefix="World Fertility for",
    args=dict(year="Year"),
    examples=["world fertility for 2023", "fertility in 1950"],
)
async def world_fertility_by_year(state: State, year: int):
    """Show a fertility world map for all countries in a given year"""
    rows = await state.query_to_tuple(
        "fert_by_year", dict(year=year), country="?", fertility=0
    )
    items = [dict(name=name, value=value) for (name, value) in rows]

    return {
        "type": "AreaMap",
        "mapId": "world",
        "colorMap": "jet",
        "items": items,
        "onClick": [
            {
                "action": "DTypeClick",
                "dtypeName": "Country",
                "idField": "selected$$area",
                "labelField": "selected$$area$label",
            },
        ],
    }


@tb.tool(
    name="Demography by Country over Time",
    ui_prefix="Demography over Time for",
    examples=[
        "Demography timeserie for Spain",
        "Italy's demography over the years",
    ],
    args={"country": "Country"},
)
async def demography_by_country_over_time(state: State, country: Country):
    """Show demography for a country over time by age group"""
    val_cols = []
    cols = [("year", "Year")]

    for name, label in DEM_DATA_COLS_AND_LABELS:
        val_cols.append(name)
        cols.append((name, label))

    rows = await state.query_to_tuple(
        "dem_by_country_code_and_type",
        dict(code=country, type=DemType.TOTAL.value),
        year=0,
        years_0_4=0,
        years_5_14=0,
        years_15_24=0,
        years_25_64=0,
        years_65_plus=0,
    )

    return {
        "type": "Series",
        "title": "Demography by Country and Year",
        "yColTitle": "Inhabitants",
        "xCol": "year",
        "xAxisType": "time",
        "valCols": val_cols,
        "smooth": False,
        "cols": cols,
        "rows": rows,
    }


@tb.context_action(tool=demography_by_country_over_time, target=Country)
def demography_by_country_over_time_for_country(ctx: ContextActionInfo):
    return {"args": {"country": ctx.value.get("label")}}


@tb.tool(
    name="Country Information",
    ui_prefix="Information for",
    examples=[
        "Information about Spain",
        "Italy's info",
    ],
    args={"country": "Country"},
)
async def country_infobox(state: State, country: Country):
    """Show information about a country"""
    row = await state.query_one(
        "get_country_by_fuzzy_name", dict(name=country), default={}
    )
    return {
        "type": "InfoBox",
        "columns": COUNTRY_TABLE_COLS_INFO,
        "row": [row.get(field[0]) for field in COUNTRY_TABLE_COLS_INFO],
    }


@tb.context_action(tool=country_infobox, target=Country)
def country_infobox_for_country(ctx: ContextActionInfo):
    return {"args": {"country": ctx.value.get("label")}}
