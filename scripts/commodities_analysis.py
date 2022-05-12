"""
functions to extract and clean World Bank commodity prices (monthly prices)
https://www.worldbank.org/en/research/commodity-markets#1

csv output in output folder as commodity_prices.csv
Values are in nominal USD
"""

import pandas as pd
from scripts import config
from numpy import nan
from typing import Optional

# ==================  PARAMETERS ======================

COMMODITY_LIST = ["Sunflower oil", "Maize", "Wheat, US HRW", "Wheat", "Palm oil"]

COMMODITY_URL = (
    "https://thedocs.worldbank.org/en/doc/5d903e848db1d1b83e0ec8f744e55570-"
    "0350012021/related/CMO-Historical-Data-Monthly.xlsx"
)

COMMODITY_DATA = pd.read_excel(COMMODITY_URL, sheet_name="Monthly Prices")
INDEX_DATA = pd.read_excel(COMMODITY_URL, sheet_name="Monthly Indices")
# =======================================================


def get_commodity_prices(commodities: list) -> pd.DataFrame:
    """
    Gets the commodity data from the World Bank and returns a clean DataFrame
    """
    # read excel
    df = COMMODITY_DATA.copy()

    # cleaning
    df.columns = df.iloc[3]
    df = (
        df.rename(columns={nan: "period"})
        .iloc[6:]
        .reset_index(drop=True)
        .rename(columns={"Rice, Thai 5%": "Rice"})
        .rename(columns={"Wheat, US HRW": "Wheat"})
        .filter(["period"] + commodities)
        .replace("..", nan)
    )

    # change date format
    df["period"] = pd.to_datetime(df.period, format="%YM%m")

    return df


def get_indices(indices: Optional[list] = None) -> pd.DataFrame:
    """gets index data from World Bank and returns a clean dataframe"""

    df = INDEX_DATA.copy()

    df = df.iloc[9:].reset_index(drop=True).replace("..", nan)
    df.columns = [
        "period",
        "Energy",
        "Non-energy",
        "Agriculture",
        "Beverages",
        "Food",
        "Oils & Meals",
        "Grains",
        "Other Food",
        "Raw Materials",
        "Timber",
        "Other Raw Mat.",
        "Fertilizers",
        "Metals & Minerals",
        "Base Metals (ex. iron ore)",
        "Precious Metals",
    ]

    # filter indices
    if indices is not None:
        indices.insert(0, "period")  # add column name for period
        df = df.loc[:, indices]

    # change date format
    df["period"] = pd.to_datetime(df.period, format="%YM%m")

    return df


def create_commodity_chart_data(df: pd.DataFrame, start_date: str) -> None:
    """Filters the data for selected start date.
    Creates a CSV formatted for Flourish."""

    df = df.loc[df.period >= start_date].reset_index(drop=True)

    df.to_csv(config.paths.output + r"/commodity_prices.csv", index=False)

    print("Successfully created commodity chart")


def create_index_chart(df: pd.DataFrame, start_date=str) -> None:
    """Filters the data for selected dated and creates a csv formatted for flourish"""

    (
        df.loc[df.period >= start_date]
        .reset_index(drop=True)
        .to_csv(f"{config.paths.output}/indices.csv", index=False)
    )

    print("Successfully created index chart")


def update_data() -> None:
    """Pipeline to update the data"""
    # Get commodity and index data
    commodity_data = get_commodity_prices(commodities=COMMODITY_LIST)
    index_data = get_indices()

    # create chart csv
    create_commodity_chart_data(df=commodity_data, start_date="2018-01-01")
    create_index_chart(df=index_data, start_date="2000-01-01")


if __name__ == "__main__":

    update_data()
