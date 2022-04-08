import pandas as pd
from scripts.config import paths
import numpy as np

from scripts.commodities_analysis import get_commodity_prices

TO_BARRELS: float = 0.1364


def read_data():
    """
    Reads the data from the file and returns a dataframe.
    """
    return pd.read_feather(f"{paths.raw_data}/baci_full_quant.feather")


def load_study_commodities():
    "Load the commodities for the study"
    return pd.read_csv(f"{paths.raw_data}/codes_pink.csv", dtype={"code": str})


def filter_commodities(df, commodities_list: list | pd.Series):
    "Filter the dataframe to keep only the commodities for study"

    return df.loc[lambda d: d.commodity_code.isin(commodities_list)].reset_index(
        drop=True
    )


def add_commodity_name_desc(df):
    "Add the commodity name and description"
    commodities = load_study_commodities()
    return df.merge(commodities, how="left", left_on="commodity_code", right_on="code")


def remove_intra_africa(df):
    "Remove the commodities from Africa"
    return df.loc[
        lambda d: ~(
            (d.importer_continent == "Africa") & (d.exporter_continent == "Africa")
        )
    ].reset_index(drop=True)


def only_african_exports(df):
    return df.loc[df.exporter_continent == "Africa"].reset_index(drop=True)


def only_african_imports(df):
    return df.loc[df.importer_continent == "Africa"].reset_index(drop=True)


def add_unit_cost(
    df,
    col_name: str = "net_import_unit_cost",
    value_col: str = "net_imports_value",
    quantity_col: str = "net_imports_quantity",
):
    df[col_name] = 1000 * df[value_col] / df[quantity_col]

    return df


def yearly_average(df, exclude: list = None):
    if exclude is None:
        exclude = ["year", "value", "quantity"]

    grouper = [x for x in df.columns if x not in exclude]

    years = f"{df.year.min()}-{df.year.max()}"

    return df.groupby(grouper).mean().reset_index().assign(year=years)


def pipeline():

    df = (
        read_data()
        .assign(
            quantity=lambda d: d.quantity.str.strip()
            .replace("NA", np.nan)
            .astype(float)
        )
        .pipe(remove_intra_africa)
        .pipe(filter_commodities, load_study_commodities().code)
        .pipe(add_commodity_name_desc)
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
            ]
        )[["value", "quantity"]]
        .sum()
        .reset_index()
    )

    # Crude correction
    df.loc[df.category == "crude oil", "quantity"] = (
        df.loc[df.category == "crude oil", "quantity"] / TO_BARRELS
    )

    # African imports
    afr_imp = (
        (
            df.pipe(only_african_imports)
            .groupby(["year", "category", "pink_sheet_commodity",], as_index=False,)
            .sum()
        )
        .pipe(yearly_average)
        .rename(
            columns={
                "value": "imports_value",
                "quantity": "imports_quantity",
                "importer": "iso_code",
            }
        )
    )

    # African exports
    afr_exp = (
        (
            df.pipe(only_african_exports)
            .groupby(["year", "category", "pink_sheet_commodity"], as_index=False)
            .sum()
        )
        .pipe(yearly_average)
        .assign(value=lambda d: -1 * d.value)
        .rename(
            columns={
                "value": "exports_value",
                "quantity": "exports_quantity",
                "exporter": "iso_code",
            }
        )
    )

    data = afr_imp.merge(
        afr_exp, on=[x for x in afr_imp.columns if x in afr_exp.columns]
    )

    data = data.assign(
        net_imports_value=lambda d: d.imports_value - d.exports_value,
        net_imports_quantity=lambda d: d.imports_quantity - d.exports_quantity,
    )

    prices = (
        get_commodity_prices(
            list(data.pink_sheet_commodity.unique()) + ["Wheat, US HRW", "Rice"]
        )
        .melt(id_vars=["period"], var_name="commodity")
        .sort_values(["period", "commodity"])
        .drop_duplicates(subset=["commodity"], keep="last")
        .set_index(["commodity"])["value"]
        .to_dict()
    )

    data = (
        data.pipe(
            add_unit_cost,
            col_name="import_cost",
            value_col="imports_value",
            quantity_col="imports_quantity",
        )
        .pipe(
        add_unit_cost,
        col_name="export_cost",
        value_col="exports_value",
        quantity_col="exports_quantity",
    )
        .pipe(add_unit_cost,)
        .assign(latest_price=lambda d: d.pink_sheet_commodity.map(prices))
        .dropna(subset=["latest_price"])
    )

    return data
