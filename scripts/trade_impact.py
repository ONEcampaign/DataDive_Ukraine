"""This file contains functions to analyse the impact of rising commodity prices in the
cost of net imports."""
import numpy as np
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

    return df.groupby(grouper).mean(numeric_only=True).reset_index().assign(year=years)


def _calc_mean_prices(
    yearly_prices: pd.DataFrame, start_year: int, end_year: int
) -> dict:
    """Calculate the mean price for each commodity"""
    return (
        yearly_prices.loc[
            (yearly_prices.year >= start_year) & (yearly_prices.year <= end_year)
        ]
        .groupby(["commodity"])
        .mean(numeric_only=True)["value"]
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
        .sum(numeric_only=True)
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
        .sum(numeric_only=True)
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
        .sum(numeric_only=True)
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
        .replace("…", np.nan, regex=True)
        .astype({"value": float})
        .assign(year=lambda d: d.period.dt.year)
        .groupby(["year", "commodity"], as_index=False)
        .mean(numeric_only=True)
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


def get_net_imports_africa(yearly: bool = False) -> None:
    """A clean version of the dataset,filtered for Africa"""

    # Read a clean version of the full dataset (quantities)
    df = read_filtered_grouped_data()

    # African imports
    afr_imp = get_african_imports(df, yearly=yearly)

    # African exports
    afr_exp = get_african_exports(df, yearly=yearly)

    # Combine imports and exports data and calculate net imports quantity
    df = calc_net_imports(
        imports_df=afr_imp, exports_df=afr_exp, column_to_net="quantity"
    )

    df.to_feather(paths.output + r"/net_imports_africa.feather")



def _commodity_evolution_df(commodity: str, exports: bool = False) -> pd.DataFrame:
    """Steps to get the change for a slope chart on Flourish"""

    df = pd.read_feather(paths.output + r"/net_imports_africa.feather")

    # filter for crude
    df = df.loc[
        lambda d: (d.pink_sheet_commodity == commodity)
        & (d.net_imports_quantity < 0 if exports else d.net_imports_quantity > 0)
    ].reset_index(drop=True)

    if exports:
        df = df.assign(net_exports_quantity=lambda d: -d.net_imports_quantity)

    # Get yearly prices
    yearly_prices = get_yearly_prices_data([commodity])

    # Convert to mean prices dict
    mean_18_20_prices = yearly_prices.pipe(_calc_mean_prices, 2018, 2020)

    # Get latest prices
    latest_prices = get_latest_prices_data([commodity])

    # Calculate spending:
    df = df.pipe(
        get_spending,
        units_columns=["net_exports_quantity"] if exports else ["net_imports_quantity"],
        prices=[("pre_crisis", mean_18_20_prices), ("latest", latest_prices)],
    )

    if exports:
        df = df.assign(
            value_net_exports_impact=lambda d: d.value_net_exports_latest
            - d.value_net_exports_pre_crisis
        )
    else:
        df = df.assign(
            value_net_imports_impact=lambda d: d.value_net_imports_latest
            - d.value_net_imports_pre_crisis
        )

    # Calculate spending by population
    col = "value_net_exports" if exports else "value_net_imports"

    df = df.pipe(utils.add_population).assign(
        value_exp_pre_pp=lambda d: d[f"{col}_pre_crisis"] / d.population,
        value_exp_latest_pp=lambda d: d[f"{col}_latest"] / d.population,
    )

    if not exports:
        df = df.rename(
            columns={
                "value_exp_pre_pp": "value_imp_pre_pp",
                "value_exp_latest_pp": "value_imp_latest_pp",
            }
        )

    # Clean for export
    return df.assign(
        country=lambda d: coco.convert(d.iso_code, to="short_name")
    ).filter(
        [
            "country",
            "iso_code",
            "net_exports_quantity",
            "net_imports_quantity",
            "value_net_exports_pre_crisis",
            "value_net_imports_pre_crisis",
            "value_net_exports_latest",
            "value_net_imports_latest",
            "value_net_exports_impact",
            "value_net_imports_impact",
            "value_exp_latest_pp",
            "value_imp_latest_pp",
            "value_imp_pre_pp",
        ],
        axis=1,
    )


def crude_evolution_chart() -> None:
    """A Flourish chart to visualise additional revenue from net crude exports, for
    selected countries"""
    df = (
        _commodity_evolution_df("Crude oil, average", exports=True)
        .loc[lambda d: d.iso_code.isin(CRUDE_COUNTRIES)]
        .drop("iso_code", axis=1)
        .assign(
            net_exports_quantity=lambda d: round(d.net_exports_quantity / 1e6, 0),
            value_net_exports_pre_crisis=lambda d: round(
                d.value_net_exports_pre_crisis / 1e9, 1
            ),
            value_net_exports_latest=lambda d: round(
                d.value_net_exports_latest / 1e9, 1
            ),
            value_net_exports_impact=lambda d: round(
                d.value_net_exports_impact / 1e9, 1
            ),
            value_exp_latest_pp=lambda d: round(d.value_exp_latest_pp, 1),
        )
        .rename(
            columns={
                "net_exports_quantity": "Net exports (million barrels)",
                "value_net_exports_pre_crisis": "Pre-war average",
                "value_net_exports_latest": "At current prices",
                "value_net_exports_impact": "Potential additional revenue",
                "value_exp_latest_pp": "Potential revenue per capita (current prices)",
            }
        )
    )

    df.to_csv(paths.output + r"/crude_oil_chart.csv", index=False)


def _flourish_commodity_pipeline(
    commodity: str,
    quantity_unit: str = "tonnes",
    quantity_rounding: float = 1,
    value_rounding: float = 1,
) -> pd.DataFrame:
    df = (
        _commodity_evolution_df(commodity, exports=False)
        .assign(commodity=commodity)
        .drop("iso_code", axis=1)
    )

    return df.assign(
        net_imports_quantity=lambda d: round(
            d.net_imports_quantity / quantity_rounding, 1
        ),
        value_net_imports_pre_crisis=lambda d: round(
            d.value_net_imports_pre_crisis / value_rounding, 1
        ),
        value_net_imports_latest=lambda d: round(
            d.value_net_imports_latest / value_rounding, 1
        ),
        value_net_imports_impact=lambda d: round(
            d.value_net_imports_impact / value_rounding, 1
        ),
        value_imp_pre_pp=lambda d: round(d.value_imp_pre_pp, 1),
        value_imp_latest_pp=lambda d: round(d.value_imp_latest_pp, 1),
    ).rename(
        columns={
            "net_imports_quantity": f"Net imports ({quantity_unit})",
            "value_net_imports_pre_crisis": "Pre-war average",
            "value_net_imports_latest": "At current prices",
            "value_net_imports_impact": "Potential additional cost",
            "value_imp_pre_pp": "Cost per capita (Pre-war prices)",
            "value_imp_latest_pp": "Potential cost per capita (current prices)",
        }
    )


def vegetable_oils_chart() -> None:
    """A Flourish chart to visualise the change in cost (in usd million and per capita)
    for palm and sunflower oils"""

    palm_oil = _flourish_commodity_pipeline(
        "Palm oil",
        value_rounding=1e6,
    )
    sunflower_oil = _flourish_commodity_pipeline(
        "Sunflower oil",
        value_rounding=1e6,
    )

    df = (
        pd.concat([palm_oil, sunflower_oil], ignore_index=True)
        .loc[lambda d: d.country != "Djibouti"]
        .groupby(["country"], as_index=False)
        .sum(numeric_only=True)
        .round(
            {
                "Net imports (tonnes)": 0,
                "Pre-war average": 1,
                "At current prices": 1,
                "Potential additional cost": 1,
                "Cost per capita (Pre-war prices)": 1,
                "Potential cost per capita (current prices)": 1,
            }
        )
        .astype({"Net imports (tonnes)": "int64"})
        .rename(
            columns={
                "Cost per capita (Pre-war prices)": "Pre-war average",
                "Potential cost per capita (current prices)": "At current prices",
            }
        )
    )

    df.to_csv(paths.output + r"/vegetable_oils_chart.csv", index=False)


def grains_chart() -> None:
    """A Flourish chart to visualise the change in cost (in usd million and per capita)
    for wheat and maize"""

    wheat = _flourish_commodity_pipeline(
        "Wheat", value_rounding=1e9, quantity_rounding=1e6
    )
    maize = _flourish_commodity_pipeline(
        "Maize", value_rounding=1e9, quantity_rounding=1e6
    )

    grain = (
        pd.concat([wheat, maize], ignore_index=True)
        .groupby(["country"], as_index=False)
        .sum(numeric_only=True)
        .filter(["country", "Pre-war average", "At current prices"], axis=1)
        .loc[lambda d: d["Pre-war average"] > 0.480]
    )

    grain_pre = (
        grain.drop(["At current prices"], axis=1)
        .assign(indicator="Pre-war average")
        .sort_values(by=["Pre-war average"], ascending=True)
        .round({"Pre-war average": 1})
        .reset_index(drop=True)
    )

    grain_pre_post = (
        grain.melt(id_vars=["country"], var_name="indicator")
        .sort_values(by="value", ascending=True)
        .round({"value": 1})
        .reset_index(drop=True)
    )

    grain_pre.to_csv(paths.output + r"/grain_chart_pre.csv", index=False)
    grain_pre_post.to_csv(paths.output + r"/grain_chart_pre_post.csv", index=False)


def analysis_pipeline():
    # Combine imports and exports data and calculate net imports quantity
    data = pd.read_feather(paths.output + r"/net_imports_africa.feather")

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


def update_trade_impact_charts() -> None:
    """Update trade impact charts for Flourish"""

    crude_evolution_chart()
    grains_chart()
    vegetable_oils_chart()


def update_all_trade() -> None:
    update_trade_impact_charts()
    analysis = analysis_pipeline()
    analysis.to_csv(paths.output + r"/data_for_analysis.csv", index=False)
    crude_evolution_chart()


if __name__ == "__main__":
    pass
    update_all_trade()
