"""
functions to extract and clean World Bank commodity prices (monthly prices)
https://www.worldbank.org/en/research/commodity-markets#1

csv output in output folder as commodity_prices.csv
Values are in nominal USD
"""

import pandas as pd
from scripts import config
from numpy import nan

# ==================  PARAMETERS ======================

COMMODITY_LIST = ["Sunflower oil", "Maize", "Wheat, US HRW", "Palm oil"]

COMMODITY_URL = (
    "https://thedocs.worldbank.org/en/doc/5d903e848db1d1b83e0ec8f744e55570-"
    "0350012021/related/CMO-Historical-Data-Monthly.xlsx"
)

# =======================================================


def get_commodity_prices(commodities: list) -> pd.DataFrame:
    """
    Gets the commodity data from the World Bank and returns a clean DataFrame
    """
    # read excel
    df = pd.read_excel(COMMODITY_URL, sheet_name="Monthly Prices")

    # cleaning
    df.columns = df.iloc[3]
    df = (
        df.rename(columns={nan: "period"})
        .iloc[6:]
        .reset_index(drop=True)
        .filter(["period"] + commodities)
        .replace("..", nan)
        .rename(columns={"Wheat, US HRW": "Wheat"})
    )

    # change date format
    df["period"] = pd.to_datetime(df.period, format="%YM%m")

    return df


def create_commodity_chart_data(df: pd.DataFrame, start_date: str) -> None:
    """Filters the data for selected start date.
    Creates a CSV formatted for Flourish."""

    df = df.loc[df.period >= start_date].reset_index(drop=True)

    df.to_csv(config.paths.output + r"/commodity_prices.csv", index=False)

    print("Successfully created commodity chart")


def update_data() -> None:
    """Pipeline to update the data"""
    # Get the commodity data
    data = get_commodity_prices(commodities=COMMODITY_LIST)

    # create chart csv
    create_commodity_chart_data(df=data, start_date="2018-01-01")


if __name__ == "__main__":

    update_data()
