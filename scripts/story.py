import pandas as pd

from scripts.read import trade_data, world_trade
from scripts.codes import (
    cereals_dict,
    fuels_dict,
    vegetable_oils_dict,
    steel_dict,
    codes_dict_bec,
    bec_names,
)


def simplify_codes(df: pd.DataFrame) -> pd.DataFrame:
    """simplify categories for easier visualisation"""

    # Create array with the commodity codes available in the data
    cc: pd.array = df.commodity_code.unique()

    # Create a combined dictionary of all the detailed codes for study
    cat = cereals_dict(cc) | fuels_dict(cc) | vegetable_oils_dict(cc) | steel_dict()

    # Create an empty dictionary for HS6 -> bec codes
    bec: dict = {}

    # Loop through all the HS6 codes to create a dictionary of HS6 -> bec codes
    # which keeps the detailed specified by 'cc' but groups the rest
    for k, v in codes_dict_bec().items():
        if k not in cat:
            cat[k] = bec_names()[v]
        bec[k] = bec_names()[v]

    # Add two columns: 'cat1' for basic bec groupings and 'cat2' for detailed + bec.
    df["cat1"] = df.commodity_code.map(bec)
    df["cat2"] = df.commodity_code.map(cat)

    return df


def simplify_exporter(
    df: pd.DataFrame,
    detailed_exporters: list = None,
    grouping_name: str = "Rest of the World",
) -> pd.DataFrame:
    """Simplify exporters. Show the exporters in 'detailed exporters' and group the
    rest under 'grouping name'. """

    # If a list is not provided, Ukraine and Russia will be default
    if detailed_exporters is None:
        detailed_exporters = ["RUS", "UKR"]

    # Assign the grouping name to all exporters which are not kept in detail
    df.loc[~df.exporter.isin(detailed_exporters), "exporter"] = grouping_name

    # grouper
    grouper = [c for c in df.columns if c not in ["value"]]

    return df.groupby(grouper, as_index=False).sum().reset_index(drop=True)


def group_by_category(df: pd.DataFrame, category_col: str) -> pd.DataFrame:
    """Group data by the category column specified"""
    return (
        df.groupby(
            [
                "year",
                "exporter",
                "importer",
                "importer_continent",
                "exporter_continent",
                category_col,
            ],
            as_index=False,
        )["value"]
        .sum()
        .sort_values(["exporter", "value"], ascending=(True, False))
    )


def average_yearly(df: pd.DataFrame) -> pd.DataFrame:
    """Average amount of trade per year, based on available data"""

    grouper: list = [c for c in df.columns if c not in ["value", "year"]]
    num_years: int = df.year.nunique()
    years: str = f"{df.year.min()}-{df.year.max()}"

    return (
        df.groupby(grouper, as_index=False)["value"]
        .sum()
        .assign(value=lambda d: round(d.value / num_years, 4), year=years)
    )


def add_exporter_share_of_total(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate the share of total trade for each country, based on selected column
    (exporter or importer)"""

    total: dict = df.groupby(["year", "importer"]).value.sum().to_dict()

    return df.assign(
        share=lambda d: d.value / d.set_index(["year", "importer"]).index.map(total)
    )


def summarise_commodity_share_of_total(
    df: pd.DataFrame, focus_column: str = "importer", commodity_column: str = "cat2"
) -> pd.DataFrame:
    """Calculate the share of total exports/imports that a particular commodity represents"""

    total: dict = df.groupby(["year", focus_column]).value.sum().to_dict()

    return (
        df.groupby(["year", focus_column, commodity_column], as_index=False)
        .value.sum()
        .assign(
            share=lambda d: d.value
            / d.set_index(["year", focus_column]).index.map(total)
        )
    )


def add_country_names(
    df: pd.DataFrame, columns: list = None, grouping_name: str = "Rest of the World"
) -> pd.DataFrame:
    """Add country names to the dataframe"""

    import country_converter as coco

    cc = coco.CountryConverter()

    if columns is None:
        columns = ["importer", "exporter"]

    for c in columns:
        df[f"{c}_name"] = cc.convert(df[c], to="short_name", not_found=grouping_name)

    return df


def exports_to_africa_data(full_data: pd.DataFrame) -> pd.DataFrame:
    """Basic pipline to produce the exports to Africa data (from Russia, Ukraine,
    and Rest of the World)"""
    return (
        full_data.pipe(simplify_codes)
        .pipe(simplify_exporter)
        .pipe(group_by_category, category_col="cat2")
        .pipe(average_yearly)
        .pipe(add_country_names)
    )


def exporters_to_africa(
    data: pd.DataFrame, step_from: int = 1, step_to: int = 2
) -> pd.DataFrame:
    """Pipeline for a flourish chart. This takes exporters and filters and groups the
    data to show the African continent as the target."""

    df = (
        data.filter(
            [
                "year",
                "exporter_name",
                "exporter_continent",
                "importer_continent",
                "value",
            ],
            axis=1,
        )
        .groupby(
            ["year", "exporter_name", "exporter_continent", "importer_continent"],
            as_index=False,
        )
        .sum()
        .assign(step_from=step_from, step_to=step_to)
        .rename(columns={"exporter_name": "source", "importer_continent": "target"})
    )

    df.loc[df.source == "Rest of the World", "source"] = df.exporter_continent

    df = (
        df.drop("exporter_continent", axis=1)
        .replace({"nan": "Other"})
        .assign(
            order=lambda d: d.source.map(
                {
                    "Ukraine": 1,
                    "Russia": 2,
                    "Europe": 3,
                    "America": 4,
                    "Asia": 5,
                    "Oceania": 6,
                }
            )
        )
        .sort_values(["order", "year", "value"], ascending=[True, False, False])
        .drop(["order",], axis=1)
        .reset_index(drop=True)
        .assign(exporter_name=lambda d: d.source)
    )

    df.to_clipboard(index=False)

    return df


def exporters_to_african_countries(
    data: pd.DataFrame, step_from: int = 1, step_to: int = 2
) -> pd.DataFrame:
    """Pipeline for a flourish chart. This takes exporters and filters and groups the
    data to show exports to each individual African country."""

    df = (
        data.filter(
            ["year", "exporter_name", "exporter_continent", "importer_name", "value",],
            axis=1,
        )
        .groupby(
            ["year", "exporter_name", "exporter_continent", "importer_name"],
            as_index=False,
        )
        .sum()
        .assign(step_from=step_from, step_to=step_to)
        .rename(columns={"exporter_name": "source", "importer_name": "target"})
    )

    df.loc[df.source == "Rest of the World", "source"] = df.exporter_continent

    df = (
        df.drop("exporter_continent", axis=1)
        .replace({"nan": "Other"})
        .assign(
            order=lambda d: d.source.map(
                {
                    "Ukraine": 1,
                    "Russia": 2,
                    "Europe": 3,
                    "America": 4,
                    "Asia": 5,
                    "Oceania": 6,
                }
            )
        )
        .sort_values(["order", "year", "value"], ascending=[True, False, False])
        .drop(["order",], axis=1)
        .reset_index(drop=True)
        .assign(importer_name=lambda d: d.target)
    )

    df.to_clipboard(index=False)

    return df


def africa_to_categories(
    data: pd.DataFrame, step_from: int = 1, step_to: int = 2
) -> pd.DataFrame:
    """Pipeline for a flourish chart. This takes 'Africa' as the source (excluding imports
     from rest of the world), and filters and groups the data to show
     the commodity categories as the target."""

    df = (
        data.loc[lambda d: d.exporter != "Rest of the World"]
        .filter(["year", "importer_continent", "cat2", "value"], axis=1)
        .groupby(["year", "importer_continent", "cat2"], as_index=False)
        .sum()
        .assign(step_from=step_from, step_to=step_to)
        .assign(
            order=lambda d: d.cat2.map(
                {
                    "Wheat": 1,
                    "Petroleum oils": 2,
                    "Coal": 3,
                    "Steel and Iron": 4,
                    "Sunflower oil": 5,
                    "Gas": 6,
                }
            ).fillna(99)
        )
        .sort_values(["year", "order", "value"], ascending=[True, True, False])
        .drop(["order",], axis=1)
        .rename(columns={"importer_continent": "source", "cat2": "target"})
        .reset_index(drop=True)
    )

    df.to_clipboard(index=False)

    return df


def africa_to_categories_by_importer(
    data: pd.DataFrame, step_from: int = 1, step_to: int = 2
) -> pd.DataFrame:
    """Pipeline for a flourish chart. This takes individual african countries
    as the source (excluding imports from rest of the world), and filters and groups
    the data to show the commodity categories as the target."""

    df = (
        data.loc[lambda d: d.exporter != "Rest of the World"]
        .filter(
            ["year", "importer_continent", "cat2", "importer_name", "value"], axis=1
        )
        .groupby(
            ["year", "importer_continent", "cat2", "importer_name"], as_index=False
        )
        .sum()
        .assign(step_from=step_from, step_to=step_to)
        .assign(
            order=lambda d: d.cat2.map(
                {
                    "Wheat": 1,
                    "Petroleum oils": 2,
                    "Coal": 3,
                    "Steel and Iron": 4,
                    "Sunflower oil": 5,
                    "Gas": 6,
                }
            ).fillna(99)
        )
        .sort_values(
            ["year", "order", "importer_name", "value"],
            ascending=[True, True, True, False],
        )
        .drop(["order",], axis=1)
        .rename(columns={"importer_continent": "source", "cat2": "target"})
        .filter(
            [
                "year",
                "source",
                "target",
                "value",
                "step_from",
                "step_to",
                "importer_name",
            ],
            axis=1,
        )
        .reset_index(drop=True)
    )

    df.to_clipboard(index=False)

    return df


def africa_to_importers(
    data: pd.DataFrame, step_from: int = 1, step_to: int = 2
) -> pd.DataFrame:
    """Pipeline for a flourish chart. This takes 'Africa' as the source (excluding imports
     from rest of the world), and filters and groups the data to show
     individual African countries as the target."""
    df = (
        data.loc[lambda d: d.exporter != "Rest of the World"]
        .filter(["year", "importer_continent", "importer_name", "value"], axis=1)
        .groupby(["year", "importer_continent", "importer_name"], as_index=False)
        .sum()
        .assign(step_from=step_from, step_to=step_to)
        .sort_values(["year", "value"], ascending=[True, False])
        .rename(columns={"importer_continent": "source", "importer_name": "target"})
        .reset_index(drop=True)
        .assign(importer_name=lambda d: d.target)
    )
    df.to_clipboard(index=False)

    return df


def categories_to_importers(
    data: pd.DataFrame, step_from: int = 1, step_to: int = 2
) -> pd.DataFrame:
    """Pipeline for a flourish chart. This takes commodity categories as the source
    (excluding imports from rest of the world), and filters and groups the data to show
     the individual African countries as the target."""

    df = (
        data.loc[lambda d: d.exporter != "Rest of the World"]
        .filter(["year", "cat2", "importer_name", "value"], axis=1)
        .groupby(["year", "cat2", "importer_name"], as_index=False)
        .sum()
        .assign(step_from=step_from, step_to=step_to)
        .sort_values(["year", "value"], ascending=[True, False])
        .rename(columns={"cat2": "source", "importer_name": "target"})
        .reset_index(drop=True)
    )

    df.to_clipboard(index=False)

    return df


def importers_to_categories(
    data: pd.DataFrame, step_from: int = 1, step_to: int = 2
) -> pd.DataFrame:
    """Pipeline for a flourish chart. This takes individual African countries as the source
    (excluding imports from rest of the world), and filters and groups the data to show
     the commodity categories as the target."""
    df = (
        data.loc[lambda d: d.exporter != "Rest of the World"]
        .filter(["year", "cat2", "importer_name", "value"], axis=1)
        .groupby(["year", "importer_name", "cat2"], as_index=False)
        .sum()
        .assign(step_from=step_from, step_to=step_to)
        .assign(
            order=lambda d: d.cat2.map(
                {
                    "Wheat": 1,
                    "Petroleum oils": 2,
                    "Coal": 3,
                    "Steel and Iron": 4,
                    "Sunflower oil": 5,
                    "Gas": 6,
                }
            ).fillna(99)
        )
        .sort_values(["year", "order", "importer_name"])
        .drop(["order",], axis=1)
        .rename(columns={"importer_name": "source", "cat2": "target"})
        .assign(importer_name=lambda d: d.source)
        .reset_index(drop=True)
    )

    df.to_clipboard(index=False)

    return df


def alluvial_overall() -> tuple[
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame
]:
    """Pipeline for the story. This takes the individual steps for the charts and creates
    dataframes. It returns a tuple of the steps in order"""
    full_data = world_trade()
    data = exports_to_africa_data(full_data)

    # World all regions to Africa
    s0 = exporters_to_africa(data, step_from=0, step_to=1)

    # Zoom into Russia and Ukraine to Africa
    s1 = s0.loc[lambda d: d.source.isin(["Ukraine", "Russia"])].copy()

    # Russia and Ukraine to Africa to commodity categories
    s2 = pd.concat([s1, africa_to_categories(data, 1, 2)], ignore_index=True)

    # Russia and Ukraine to Africa to individual countries
    s3 = pd.concat([s1, africa_to_importers(data, 1, 3)], ignore_index=True)

    # Russia and Ukraine to individual African countries (directly)
    s4 = exporters_to_african_countries(data, 0, 4).loc[
        lambda d: d.source.isin(["Ukraine", "Russia"])
    ]

    # Individual African countries to commodity categories
    s5 = pd.concat(
        [
            exporters_to_african_countries(data, 0, 3).loc[
                lambda d: d.source.isin(["Ukraine", "Russia"])
            ],
            importers_to_categories(data, 3, 4),
        ],
        ignore_index=True,
    )

    return s0, s1, s2, s3, s4, s5


if __name__ == "__main__":
    pass
    chart1, chart2, chart3, chart4, chart5, chart6 = alluvial_overall()
