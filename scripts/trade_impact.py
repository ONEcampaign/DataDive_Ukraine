"""This file contains functions to analyse the impact of rising commodity prices in the
cost of net imports."""

import pandas as pd
from scripts.config import paths
import numpy as np

from scripts.commodities_analysis import get_commodity_prices

TO_BARRELS: float = 0.1364


def _read_raw_data():
    """
    Reads the data from the file and returns a dataframe.
    """
    return pd.read_feather(f"{paths.raw_data}/baci_full.feather")


def _load_study_commodities():
    """Load the commodities for the study"""
    return pd.read_csv(f"{paths.raw_data}/codes_pink.csv", dtype={"code": str})


def _filter_commodities(df, commodities_list: list | pd.Series):
    """Filter the dataframe to keep only the commodities for study"""

    return df.loc[lambda d: d.commodity_code.isin(commodities_list)].reset_index(
        drop=True
    )


def _add_commodity_name_desc(df):
    """Add the commodity name and description"""
    commodities = _load_study_commodities()
    return df.merge(commodities, how="left", left_on="commodity_code", right_on="code")


def _remove_intra_africa(df):
    """Remove intra-african trade - particularly important if showing aggregate
    African figures"""
    return df.loc[
        lambda d: ~(
            (d.importer_continent == "Africa") & (d.exporter_continent == "Africa")
        )
    ].reset_index(drop=True)


def _only_african_exports(df):
    """Filter a dataframe to keep only exports originating in African countries"""
    return df.loc[df.exporter_continent == "Africa"].reset_index(drop=True)


def _only_african_imports(df):
    """Filter a dataframe to keep only imports originating in African countries"""
    return df.loc[df.importer_continent == "Africa"].reset_index(drop=True)


def _add_unit_cost(
    df: pd.DataFrame,
    col_name: str = "net_import_unit_cost",
    value_col: str = "net_imports_value",
    quantity_col: str = "net_imports_quantity",
):
    """Calculate the implied unit cost of the trade, derived from the value and quantity
    data. Value, quantity and target column names are optional parameters."""

    df[col_name] = 1000 * df[value_col] / df[quantity_col]

    return df


def _yearly_average(df, exclude: list = None):
    """Calculate the yearly average of a dataframe. A list of columns can be excluded
    from the grouping operation. By default these are 'year','value',' and 'quantity'"""
    if exclude is None:
        exclude = ["year", "value", "quantity"]

    grouper = [x for x in df.columns if x not in exclude]

    years = f"{df.year.min()}-{df.year.max()}"

    return df.groupby(grouper).mean().reset_index().assign(year=years)


def read_filtered_grouped_data() -> pd.DataFrame:
    """STEP 1: Read data, filter for commodities of interest, add commodity
     descriptions and aggregate by year, exporter, importer, and commodity.
     This includes changing oils from mt to barrels"""

    df = (
        _read_raw_data()
        .pipe(_filter_commodities, commodities_list=_load_study_commodities().code)
        .pipe(_add_commodity_name_desc)
        .groupby(
            [
                "year",
                "exporter",
                "importer",
                "commodity_code",
                "importer_continent",
                "exporter_continent",
                "category",
                "pink_sheet_commodity",
            ],
            as_index=False,
        )["quantity"]
        .sum()
    )

    df.loc[df.category == "crude oil", "quantity"] = (
        df.loc[df.category == "crude oil", "quantity"] / TO_BARRELS
    )

    return df


def get_african_imports(df: pd.DataFrame) -> pd.DataFrame:
    """STEP 2: Filter for African imports"""
    return (
        df.pipe(_only_african_imports)
        .groupby(
            ["year", "importer", "category", "pink_sheet_commodity",], as_index=False,
        )
        .sum()
        .pipe(_yearly_average, exclude=["year", "quantity"])
        .rename(columns={"quantity": "imports_quantity", "importer": "iso_code",})
    )


def get_african_exports(df: pd.DataFrame) -> pd.DataFrame:
    """STEP 3: Filter for African exports"""
    return (
        df.pipe(_only_african_exports)
        .groupby(
            ["year", "exporter", "category", "pink_sheet_commodity"], as_index=False
        )
        .sum()
        .pipe(_yearly_average, exclude=["year", "quantity"])
        .assign(quantity=lambda d: -1 * d.quantity)
        .rename(columns={"quantity": "exports_quantity", "exporter": "iso_code",})
    )


def get_net_imports(
    imports_df: pd.DataFrame, exports_df: pd.DataFrame, column_to_net: str = "quantity"
) -> pd.DataFrame:
    """STEP 4: Calculate net imports"""

    # Combine the dataframes
    df = imports_df.merge(
        exports_df, on=[x for x in imports_df.columns if x in exports_df.columns]
    )

    # Calculate the net quantity
    return (
        df.assign(
            net_imports=lambda d: d[f"imports_{column_to_net}"]
            + d[f"exports_{column_to_net}"]
        )
        .rename(columns={"net_imports": f"net_imports_{column_to_net}"})
        .reset_index(drop=True)
    )


def get_yearly_prices_data(commodities_list: list) -> pd.DataFrame:

    return (
        get_commodity_prices(commodities_list)
        .melt(id_vars=["period"], var_name="commodity")
        .assign(year=lambda d: d.period.dt.year)
        .groupby(["year", "commodity"], as_index=False)
        .mean()
    )


def get_latest_prices_data(commodities_list: list) -> dict:

    return (
        get_commodity_prices(commodities_list)
        .melt(id_vars=["period"], var_name="commodity")
        .sort_values(["period", "commodity"])
        .drop_duplicates(subset=["commodity"], keep="last")
        .set_index("commodity")["value"]
        .to_dict()
    )


def get_spending(
    df: pd.DataFrame, units_columns: list, prices: list[tuple[str, int]]
) -> pd.DataFrame:
    """ Calculate spending for commodities for all variables,
    based on average yearly prices and latest prices"""

    for column in units_columns:
        df[f"value_{column}_{prices[0][0]}"] = df[column] * df.pink_sheet_commodity.map(
            prices[0][1]
        )
        df[f"value_{column}_{prices[1][0]}"] = df[column] * df.pink_sheet_commodity.map(
            prices[1][1]
        )

    return df


def pipeline():

    # Read a clean version of the full dataset (quantities)
    df = read_filtered_grouped_data()

    # African imports
    afr_imp = get_african_imports(df)

    # African exports
    afr_exp = get_african_exports(df)

    # STEP 4: Combine imports and exports data and calculate net imports quantity
    data = get_net_imports(
        imports_df=afr_imp, exports_df=afr_exp, column_to_net="quantity"
    )

    # Get the list of commodities for study
    commodities = list(data.pink_sheet_commodity.unique()) + ["Wheat, US HRW", "Rice"]

    # Get yearly average prices
    yearly_prices = get_yearly_prices_data(commodities)
    mean_18_20_prices = (
        yearly_prices.loc[(yearly_prices.year >= 2018) & (yearly_prices.year <= 2020)]
        .groupby(["commodity"])
        .mean()["value"]
        .to_dict()
    )

    # Get the latest (monthly) prices
    latest_prices = get_latest_prices_data(commodities)

    # STEP 5: Calculate spending for commodities for all variables
    data = data.pipe(
        get_spending,
        units_columns=["imports_quantity", "exports_quantity", "net_imports_quantity"],
        prices=[("pre-crisis", mean_18_20_prices), ("latest", latest_prices)],
    )

    return data


if __name__ == "__main__":
    analysis = pipeline()
