import pandas as pd

from scripts.config import paths
from scripts.read_data import world_trade_all_importers, world_trade_africa
from scripts.codes import (
    cereals_dict,
    fuels_dict,
    vegetable_oils_dict,
    steel_dict,
    weapons_dict,
    potash_dict,
    codes_dict_bec,
    bec_names,
)


def simplify_codes(df: pd.DataFrame) -> pd.DataFrame:
    """simplify categories for easier visualisation"""

    # Create array with the commodity codes available in the data
    cc: pd.array = df.commodity_code.unique()

    # Create a combined dictionary of all the detailed codes for study
    cat = (
        cereals_dict(cc)
        | fuels_dict(cc)
        | vegetable_oils_dict(cc)
        | steel_dict()
        | weapons_dict()
        | potash_dict(cc)
    )

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
    rest under 'grouping name'."""

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
        df.groupby(
            ["year", "exporter_name", focus_column, commodity_column], as_index=False
        )
        .value.sum()
        .assign(
            share=lambda d: d.value
            / d.set_index(["year", focus_column]).index.map(total)
        )
    )


def gdp_dict() -> dict:
    return (
        pd.read_csv(paths.raw_data + r"/2019gdp.csv")
        .set_index(["iso_code"])["value"]
        .to_dict()
    )


def summarise_commodity_source_share(
    df: pd.DataFrame,
    commodity_column: str = "cat2",
) -> pd.DataFrame:
    """Calculate the share of total that a source represents for each commodity"""

    total: dict = (
        df.groupby(["year", "importer_name", commodity_column]).value.sum().to_dict()
    )

    return (
        df.groupby(
            ["year", "exporter_name", "importer", "importer_name", commodity_column],
            as_index=False,
        )
        .value.sum()
        .assign(
            share=lambda d: d.value
            / d.set_index(["year", "importer_name", commodity_column]).index.map(total)
        )
        # .loc[lambda d: d.exporter_name != "Rest of the World"]
        # .groupby(["year", "importer", "importer_name", "cat2"], as_index=False)
        # .sum()
        .sort_values(["importer_name", "share"], ascending=(True, False))
        .assign(
            value=lambda d: round(d.value / 1e3, 1),
            # share = lambda d: d.apply(lambda x: f'{round(100*x.share,1)}%', axis=1)
            share=lambda d: round(100 * d.share, 1),
            # gdp_share=lambda d: round(100 * d.value / d.importer.map(gdp_dict()), 3),
        )
        .reset_index(drop=True)
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


def exports_no_intra_europe(full_data: pd.DataFrame) -> pd.DataFrame:
    """Basic pipline to produce the exports to Africa data (from Russia, Ukraine,
    and Rest of the World)"""
    import country_converter as coco

    cc = coco.CountryConverter()

    eu = cc.data.set_index("ISO3")["EU27"].to_dict()

    return (
        full_data.pipe(simplify_codes)
        .assign(
            eu_exporter=lambda d: d.exporter.map(eu),
            eu_importer=lambda d: d.importer.map(eu),
        )
        .loc[lambda d: ~((d.eu_exporter == "EU27") & (d.eu_importer == "EU27"))]
        .drop(columns=["eu_exporter", "eu_importer"])
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
        .drop(
            [
                "order",
            ],
            axis=1,
        )
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
            [
                "year",
                "exporter_name",
                "exporter_continent",
                "importer_name",
                "value",
            ],
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
        .drop(
            [
                "order",
            ],
            axis=1,
        )
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
        .drop(
            [
                "order",
            ],
            axis=1,
        )
        .rename(columns={"importer_continent": "source", "cat2": "target"})
        .reset_index(drop=True)
    )

    df.to_clipboard(index=False)

    return df


def exporter_to_categories(
    data: pd.DataFrame, step_from: int = 1, step_to: int = 2
) -> pd.DataFrame:
    """Pipeline for a flourish chart. This takes 'Africa' as the source (excluding imports
    from rest of the world), and filters and groups the data to show
    the commodity categories as the target."""

    df = (
        data.filter(["year", "exporter", "importer_continent", "cat2", "value"], axis=1)
        .groupby(["year", "exporter", "importer_continent", "cat2"], as_index=False)
        .sum()
        .assign(step_from=step_from, step_to=step_to)
        .assign(
            order=lambda d: d.cat2.map(
                {
                    "Wheat": 1,
                    "Sunflower oil": 2,
                    "Coal": 3,
                    "Barley": 4,
                    "Maize": 5,
                    "Steel and Iron": 6,
                    "Petroleum oils": 7,
                }
            ).fillna(99)
        )
        .sort_values(
            ["year", "exporter", "order", "value"], ascending=[True, False, True, False]
        )
        .drop(
            [
                "order",
            ],
            axis=1,
        )
        .rename(columns={"exporter": "source", "cat2": "target"})
        .reset_index(drop=True)
    )

    df.to_clipboard(index=False)

    return df


def exporter_to_categories_by_importer(
    data: pd.DataFrame, step_from: int = 1, step_to: int = 2
) -> pd.DataFrame:
    """Pipeline for a flourish chart. This takes 'Africa' as the source (excluding imports
    from rest of the world), and filters and groups the data to show
    the commodity categories as the target."""

    df = (
        data.filter(["year", "exporter_name", "importer_name", "cat2", "value"], axis=1)
        .groupby(["year", "exporter_name", "importer_name", "cat2"], as_index=False)
        .sum()
        .assign(step_from=step_from, step_to=step_to)
        .assign(
            order=lambda d: d.cat2.map(
                {
                    "Wheat": 1,
                    "Sunflower oil": 2,
                    "Coal": 3,
                    "Barley": 4,
                    "Maize": 5,
                    "Steel and Iron": 6,
                    "Petroleum oils": 7,
                }
            ).fillna(99)
        )
        .sort_values(
            ["year", "exporter_name", "order", "value"],
            ascending=[True, False, True, False],
        )
        .drop(
            [
                "order",
            ],
            axis=1,
        )
        .rename(columns={"exporter_name": "source", "cat2": "target"})
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
        .drop(
            [
                "order",
            ],
            axis=1,
        )
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
        .drop(
            [
                "order",
            ],
            axis=1,
        )
        .rename(columns={"importer_name": "source", "cat2": "target"})
        .assign(importer_name=lambda d: d.source)
        .reset_index(drop=True)
    )

    df.to_clipboard(index=False)

    return df


def data_as_pct(exports_data: pd.DataFrame) -> pd.DataFrame:
    """Convert value to percentage, based on exporter and commodity"""

    return (
        exports_data.replace(
            {"exporter": {"RUS": "Russia and Ukraine", "UKR": "Russia and Ukraine"}}
        )
        .groupby(["year", "exporter", "cat2"], as_index=False)
        .sum()
        .assign(
            value=lambda d: round(
                100 * d.value / d.groupby(["year", "cat2"])["value"].transform("sum"),
                2,
            )
        )
    )


def flourish_story() -> None:
    """Pipeline for the story. This takes the individual steps for the charts and creates
    dataframes. It returns a tuple of the steps in order"""

    all_importers_data = world_trade_all_importers()

    # SLIDES 2 and 3: Major share of key commodity exports
    commodity_exp_share = (
        exports_no_intra_europe(all_importers_data)
        .pipe(summarise_commodity_source_share, "cat2")
        .groupby(["year", "exporter_name", "cat2"], as_index=False)["value"]
        .sum()
        .assign(
            share=lambda d: round(
                100 * d.value / d.groupby(["year", "cat2"])["value"].transform("sum"), 1
            )
        )
        .loc[
            lambda d: d.cat2.isin(
                [
                    "Barley",
                    "Maize",
                    "Petroleum oils",
                    "Sunflower oil",
                    "Wheat",
                    "Coal",
                    "Steel and Iron",
                    "Potash",
                ]
            )
        ]
        .loc[lambda d: d.exporter_name.isin(["Ukraine", "Russia"])]
        .pivot(index="cat2", columns="exporter_name", values="share")
    )

    # Trade with Africa
    africa_trade = world_trade_africa()
    data = exports_to_africa_data(africa_trade)

    # SLIDE 7: Exports to Africa
    df = (
        exporters_to_africa(data, step_from=0, step_to=1)
        .loc[lambda d: d.source != "Africa"]
        .assign(
            pct=lambda d: round(
                100 * d.value / d.groupby(["year"])["value"].transform("sum"), 2
            )
        )
    )

    # SLIDE 8: Exports to Africa, zoom Ukraine and Russia
    df_zoom = df.loc[lambda d: d.source.isin(["Ukraine", "Russia"])].copy()

    # SLIDE 9: Ukraine and Russia to African countries
    to_african = exporters_to_african_countries(data, 0, 4).loc[
        lambda d: d.source.isin(["Ukraine", "Russia"])
    ]

    # SLIDE 12 and 13: Russia and Ukraine to commodity categories
    df_commodity = (
        exporter_to_categories(data, 0, 2)
        .replace({"source": {"RUS": "Russia", "UKR": "Ukraine"}})
        .drop(["importer_continent"], axis=1)
        .assign(
            share=lambda d: round(
                100 * d.value / d.groupby(["year", "target"])["value"].transform("sum"),
                2,
            )
        )
        .loc[lambda d: d.source != "Rest of the World"]
    )

    # Russia and Ukraine (together) to African countries by category
    afr_cat = (
        exporter_to_categories_by_importer(data, 0, 4)
        .assign(
            share=lambda d: round(
                100
                * d.value
                / d.groupby(["year", "target", "importer_name"])["value"].transform(
                    "sum"
                ),
                2,
            )
        )
        .loc[lambda d: d.source != "Rest of the World"]
        .groupby(["year", "importer_name", "target"], as_index=False)[
            ["value", "share"]
        ]
        .sum()
    )

    # SLIDE 14 : wheat
    wheat = (
        afr_cat.loc[lambda d: d.target == "Wheat"]
        .copy()
        .assign(value=lambda d: round(d.value / 1e3, 1))
        .assign(value=lambda d: d.value.astype(str) + "m")
    )
    # SLIDE 15: barley
    barley = (
        afr_cat.loc[lambda d: d.target == "Barley"]
        .copy()
        .assign(value=lambda d: round(d.value / 1e3, 1))
        .assign(value=lambda d: d.value.astype(str) + "m")
    )


if __name__ == "__main__":
    pass
