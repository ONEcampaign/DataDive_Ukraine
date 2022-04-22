"""This file contains functions to analyse the impact of rising commodity prices in the
cost of net imports."""

import pandas as pd
from scripts.config import paths
from scripts import utils
import country_converter as coco

from scripts.commodities_analysis import get_commodity_prices

TO_BARRELS: float = 0.1364
CRUDE_COUNTRIES = ["NGA", "AGO", "LBY", "DZA", "COG", "EGY"]


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

    years = f"{df.year.min()}-{df.year.max()} (mean)"

    return df.groupby(grouper).mean().reset_index().assign(year=years)


def _calc_mean_prices(
    yearly_prices: pd.DataFrame, start_year: int, end_year: int
) -> dict:
    """Calculate the mean price for each commodity"""
    return (
        yearly_prices.loc[
            (yearly_prices.year >= start_year) & (yearly_prices.year <= end_year)
        ]
        .groupby(["commodity"])
        .mean()["value"]
        .to_dict()
    )


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


def get_african_imports(df: pd.DataFrame, yearly: bool = False) -> pd.DataFrame:
    """STEP 2: Filter for African imports"""
    df = (
        df.pipe(_only_african_imports)
        .groupby(
            ["year", "importer", "category", "pink_sheet_commodity"],
            as_index=False,
        )
        .sum()
    )

    if not yearly:
        df = df.pipe(_yearly_average, exclude=["year", "quantity"])

    return df.rename(columns={"quantity": "imports_quantity", "importer": "iso_code"})


def get_african_exports(df: pd.DataFrame, yearly: bool = False) -> pd.DataFrame:
    """STEP 3: Filter for African exports"""
    df = (
        df.pipe(_only_african_exports)
        .groupby(
            ["year", "exporter", "category", "pink_sheet_commodity"], as_index=False
        )
        .sum()
    )
    if not yearly:
        df = df.pipe(_yearly_average, exclude=["year", "quantity"])

    return df.assign(quantity=lambda d: -1 * d.quantity).rename(
        columns={"quantity": "exports_quantity", "exporter": "iso_code"}
    )


def calc_net_imports(
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


def add_total_trade(
    df: pd.DataFrame,
    import_column: str = "imports_quantity",
    export_column: str = "exports_quantity",
) -> pd.DataFrame:
    """ """

    df["total_trade"] = df[import_column] + (df[export_column] * -1)
    return df


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
    """Calculate spending for commodities for all variables,
    based on average yearly prices and latest prices"""

    for column in units_columns:
        df[f"value_{column}_{prices[0][0]}"] = df[column] * df.pink_sheet_commodity.map(
            prices[0][1]
        )
        df[f"value_{column}_{prices[1][0]}"] = df[column] * df.pink_sheet_commodity.map(
            prices[1][1]
        )

    return df.rename(
        columns={k: k.replace("_quantity", "") for k in df.columns if "value" in k}
    )


def get_net_imports_africa(yearly: bool = False) -> pd.DataFrame:
    """A clean version of the dataset,filtered for Africa"""

    # Read a clean version of the full dataset (quantities)
    df = read_filtered_grouped_data()

    # African imports
    afr_imp = get_african_imports(df, yearly=yearly)

    # African exports
    afr_exp = get_african_exports(df, yearly=yearly)

    # Combine imports and exports data and calculate net imports quantity
    return calc_net_imports(
        imports_df=afr_imp, exports_df=afr_exp, column_to_net="quantity"
    )


def crude_evolution_chart() -> None:
    """A Flourish chart to visualise additional revenue from net crude exports, for
    selected countries"""

    df = get_net_imports_africa()

    # filter for crude
    df = (
        df.loc[lambda d: (d.category == "crude oil") & (d.net_imports_quantity < 0)]
        .loc[lambda d: d.iso_code.isin(CRUDE_COUNTRIES)]
        .assign(net_exports_quantity=lambda d: -d.net_imports_quantity)
        .reset_index(drop=True)
    )
    # Get yearly prices
    yearly_prices = get_yearly_prices_data(["Crude oil, average"])

    # Convert to mean prices dict
    mean_18_20_prices = yearly_prices.pipe(_calc_mean_prices, 2018, 2020)

    # Get latest prices
    latest_prices = get_latest_prices_data(["Crude oil, average"])

    # Calculate spending:
    df = df.pipe(
        get_spending,
        units_columns=["net_exports_quantity"],
        prices=[("pre_crisis", mean_18_20_prices), ("latest", latest_prices)],
    ).assign(
        value_net_exports_impact=lambda d: d.value_net_exports_latest
        - d.value_net_exports_pre_crisis
    )

    # Calculate spending by population
    df = df.pipe(utils.add_population).assign(
        value_exp_pre_pp=lambda d: d.value_net_exports_pre_crisis / d.population,
        value_exp_latest_pp=lambda d: d.value_net_exports_latest / d.population,
    )

    # Clean for export
    df = (
        df.assign(country=lambda d: coco.convert(d.iso_code, to="short_name"))
        .filter(
            [
                "country",
                "net_exports_quantity",
                "value_net_exports_pre_crisis",
                "value_net_exports_latest",
                "value_net_exports_impact",
                "value_exp_latest_pp",
            ],
            axis=1,
        )
        .assign(
            net_exports_quantity=lambda d: d.net_exports_quantity / 1e6,
            value_net_exports_pre_crisis=lambda d: d.value_net_exports_pre_crisis / 1e9,
            value_net_exports_latest=lambda d: d.value_net_exports_latest / 1e9,
            value_net_exports_impact=lambda d: d.value_net_exports_impact / 1e9,
        )
        .rename(
            columns={
                "net_exports_quantity": "Net exports (million barrels)",
                "value_net_exports_pre_crisis": "Average revenue (pre-war)",
                "value_net_exports_latest": "Potential revenue (current prices)",
                "value_net_exports_impact": "Potential additional revenue",
                "value_exp_latest_pp": "Potential revenue per capita (current prices)",
            }
        )
    )


def pipeline():

    # Combine imports and exports data and calculate net imports quantity
    data = get_net_imports_africa()

    # Add total trade
    data = data.pipe(add_total_trade)

    # Get the list of commodities for study
    commodities = list(data.pink_sheet_commodity.unique()) + ["Wheat, US HRW", "Rice"]

    # Get yearly average prices
    yearly_prices = get_yearly_prices_data(commodities)
    mean_18_20_prices = yearly_prices.pipe(_calc_mean_prices, 2018, 2020)

    # Get the latest (monthly) prices
    latest_prices = get_latest_prices_data(commodities)

    # STEP 5: Calculate spending for commodities for all variables
    data = data.pipe(
        get_spending,
        units_columns=[
            "imports_quantity",
            "exports_quantity",
            "net_imports_quantity",
            "total_trade",
        ],
        prices=[("pre_crisis", mean_18_20_prices), ("latest", latest_prices)],
    )

    # STEP 6: Add net impact (value)
    data = data.assign(
        net_imports_impact_value=lambda d: d.value_net_imports_latest
        - d.value_net_imports_pre_crisis
    )

    # STEP 7: Add other categorical information about countries

    data = (
        data.pipe(utils.add_ppp, usd_values_col="value_imports_pre_crisis")
        .pipe(utils.add_ppp, usd_values_col="value_imports_latest")
        .pipe(utils.add_ppp, usd_values_col="value_exports_pre_crisis")
        .pipe(utils.add_ppp, usd_values_col="value_exports_latest")
        .pipe(utils.add_ppp, usd_values_col="value_net_imports_pre_crisis")
        .pipe(utils.add_ppp, usd_values_col="value_net_imports_latest")
        .pipe(utils.add_ppp, usd_values_col="net_imports_impact_value")
        .pipe(utils.add_population)
        .pipe(utils.add_gdp)
        .pipe(utils.add_income_levels)
        .assign(country=lambda d: coco.convert(d.iso_code, to="short_name"))
    )

    # STEP 8: Reshape for analysis
    idx = [
        "iso_code",
        "country",
        "category",
        "pink_sheet_commodity",
        "year",
        "population",
        "gdp",
        "income_level",
    ]
    data = data.melt(id_vars=idx, var_name="indicator")

    return data


if __name__ == "__main__":
    pass
    # analysis = pipeline()
    # analysis.to_csv(paths.output + r"/data_for_analysis.csv", index=False)
