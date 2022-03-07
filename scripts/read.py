"""This file contains functions which can read data downloaded from CEPII, do some
preprocessing and save to a feather file. It also contains functions to read the data
as a pandas dataframe."""

import pandas as pd
from scripts.config import paths

import country_converter as coco


def _countries_dict() -> dict:
    """Returns a dictionary with country codes as keys and ISO3 as values"""
    countries = pd.read_csv(f"{paths.raw_data}/country_codes.csv")
    return countries.set_index("country_code")["iso_3digit_alpha"].to_dict()


def _continents_dict() -> dict:
    cc = coco.CountryConverter()
    return cc.data.set_index("ISO3")["continent"].to_dict()


def baci2feather(year: int = 2020) -> None:
    """
    Read a CEPPI CSV for a specific year. Performs basic cleaning:
    -Renames the columns to human-readable names.
    -Adds iso3 codes to exporters and importers
    -Filters only for valid exporter countries (by iso code)
    -Drops the quantity column
    """

    df = (
        pd.read_csv(f"{paths.raw_data}/hs17_{year}.csv", dtype={"k": str},
                    keep_default_na=False)
        .rename(
            columns={
                "t": "year",
                "i": "exporter",
                "j": "importer",
                "v": "value",
                "k": "commodity_code",
                "q": "quantity",
            }
        )
        .drop(["quantity"], axis=1)
        .assign(
            exporter=lambda d: d.exporter.map(_countries_dict()),
            importer=lambda d: d.importer.map(_countries_dict()),
        )
        .dropna(subset=["importer"])
        .assign(
            importer_continent=lambda d: d.importer.map(_continents_dict()),
            exporter_continent=lambda d: d.exporter.map(_continents_dict()),
        )
        .astype(
            {
                "year": "int16",
                "exporter": "str",
                "importer": "str",
                "commodity_code": "str",
                "importer_continent": "str",
                "exporter_continent": "str",
            }
        )
        .reset_index(drop=True)
    )

    df.to_feather(f"{paths.raw_data}/hs17_{year}.feather")


def read_baci(year: int = 2020) -> pd.DataFrame:
    """Read the feather file for a specific year"""
    return pd.read_feather(f"{paths.raw_data}/hs17_{year}.feather")


def filter_africa(df: pd.DataFrame, imp_exp: str = "exporter") -> pd.DataFrame:
    """Filter the dataframe for Africa. Can specify if Africa is the importer or exporter"""
    if imp_exp not in ["exporter", "importer"]:
        raise ValueError('imp_exp must be either "exporter" or "importer"')

    return df.loc[df[f"{imp_exp}_continent"] == "Africa"].reset_index(drop=True)


def filter_exporter(df: pd.DataFrame, exporter_iso: str | list) -> pd.DataFrame:
    """Filter the dataframe for a specific exporter"""
    if isinstance(exporter_iso, str):
        exporter_iso = [exporter_iso]

    if not any(exp in exporter_iso for exp in df.exporter):
        raise ValueError(f"Exporter(s) {exporter_iso} not in dataframe")

    return df.loc[df["exporter"].isin(exporter_iso)].reset_index(drop=True)


def trade_data() -> pd.DataFrame:
    """A DataFrame of BACI data for 2018-2020, filtered for Africa as importer,
    and Ukraine and Russia as exporters"""

    df = pd.concat([read_baci(year) for year in range(2018, 2021)], ignore_index=True)

    return df.pipe(filter_africa, imp_exp="importer").pipe(
        filter_exporter, ["RUS", "UKR"]
    )


def world_trade() -> pd.DataFrame:
    df = pd.concat([read_baci(year) for year in range(2018, 2021)], ignore_index=True)

    return df.pipe(filter_africa, imp_exp="importer")


if __name__ == "__main__":
    [baci2feather(year) for year in range(2018, 2021)]
    data = world_trade()
