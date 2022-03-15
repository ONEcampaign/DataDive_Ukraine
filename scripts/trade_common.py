import pandas as pd
from scripts import codes

from scripts.config import paths

# ====================   PARAMETERS  ====================

GDP_FILE_NAME: str = "2019gdp.csv"

# =======================================================


def simplify_codes(df: pd.DataFrame) -> pd.DataFrame:
    """simplify categories for easier visualisation"""

    # Create array with the commodity codes available in the data
    cc: pd.array = df.commodity_code.unique()

    # Create a combined dictionary of all the detailed codes for study
    cat = (
        codes.cereals_dict(cc)
        | codes.fuels_dict(cc)
        | codes.vegetable_oils_dict(cc)
        | codes.steel_dict()
        | codes.weapons_dict()
        | codes.potash_dict()
    )

    # Create an empty dictionary for HS6 -> bec codes
    bec: dict = {}

    # Loop through all the HS6 codes to create a dictionary of HS6 -> bec codes
    # which keeps the detailed specified by 'cc' but groups the rest
    for k, v in codes.codes_dict_bec().items():
        if k not in cat:
            cat[k] = codes.bec_names()[v]
        bec[k] = codes.bec_names()[v]

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


def gdp_dict() -> dict:
    return (
        pd.read_csv(paths.raw_data + fr"/{GDP_FILE_NAME}")
        .set_index(["iso_code"])["value"]
        .to_dict()
    )


def eu_27_dict() -> dict:
    import country_converter as coco

    cc = coco.CountryConverter()

    return cc.data.set_index("ISO3")["EU27"].to_dict()


def summarise_commodity_source_share(
    df: pd.DataFrame, commodity_column: str = "cat2",
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
        .sort_values(["importer_name", "share"], ascending=(True, False))
        .assign(
            value=lambda d: round(d.value / 1e3, 1),
            share=lambda d: round(100 * d.share, 1),
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

    return (
        full_data.pipe(simplify_codes)
        .assign(
            eu_exporter=lambda d: d.exporter.map(eu_27_dict()),
            eu_importer=lambda d: d.importer.map(eu_27_dict()),
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
        .drop(["order",], axis=1,)
        .reset_index(drop=True)
        .assign(exporter_name=lambda d: d.source)
    )

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
        .drop(["order",], axis=1,)
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
        .drop(["order",], axis=1,)
        .rename(columns={"importer_continent": "source", "cat2": "target"})
        .reset_index(drop=True)
    )

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
        .drop(["order",], axis=1,)
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
        .drop(["order"], axis=1,)
        .rename(columns={"exporter_name": "source", "cat2": "target"})
        .reset_index(drop=True)
    )

    return df
