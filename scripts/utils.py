""" """

import pandas as pd
import wbgapi as wb
import weo
from scripts import config

# =============================================================================
#  Parameters
# =============================================================================

WEO_YEAR: int = 2022
WEO_RELEASE: int = 1
GDP_YEAR: int = 2021


# =============================================================================
#  population
# =============================================================================


def get_wb_indicator(indicator: str) -> pd.DataFrame:
    """query wb api using a specific indicator code"""
    try:
        return wb.data.DataFrame(
            indicator,
            mrnev=1,
            numericTimeKeys=True,
            labels=False,
            columns="series",
            timeColumns=True,
        )
    except ConnectionError:
        raise ConnectionError(f"Could not retrieve indicator {indicator}")


def wb_indicator_to_dict(df: pd.DataFrame, indicator_col: str):
    """converts a wb series to a dictionary where keys are iso3 codes and values
    are indicator values"""
    return df[indicator_col].astype("int32").to_dict()


def add_population(df: pd.DataFrame, iso_codes_col: str = "iso_code") -> pd.DataFrame:
    """Adds population to a dataframe"""

    # population indicator ID
    id_ = "SP.POP.TOTL"

    # get population data
    pop_: dict = get_wb_indicator(id_).pipe(wb_indicator_to_dict, id_)

    return df.assign(population=lambda d: d[iso_codes_col].map(pop_))


# =============================================================================
#  IMF
# =============================================================================


def _download_weo(year: int, release: int) -> None:
    """Downloads WEO as a csv to glossaries folder as "weo_month_year.csv"""

    try:
        weo.download(
            year=year,
            release=release,
            directory=config.paths.raw_data,
            filename=f"weo_{year}_{release}.csv",
        )
    except ConnectionError:
        raise ConnectionError("Could not download weo data")


def _clean_weo(df: pd.DataFrame) -> pd.DataFrame:
    """cleans and formats weo dataframe"""

    columns = {
        "ISO": "iso_code",
        "WEO Subject Code": "indicator",
        "Subject Descriptor": "indicator_name",
        "Units": "units",
        "Scale": "scale",
    }
    cols_to_drop = [
        "WEO Country Code",
        "Country",
        "Subject Notes",
        "Country/Series-specific Notes",
        "Estimates Start After",
    ]
    return (
        df.drop(cols_to_drop, axis=1)
        .rename(columns=columns)
        .melt(id_vars=columns.values(), var_name="year", value_name="value")
        .assign(
            value=lambda d: d.value.map(
                lambda x: str(x).replace(",", "").replace("-", "")
            )
        )
        .astype({"year": "int32"})
        .assign(value=lambda d: pd.to_numeric(d.value, errors="coerce"))
    )


def get_gdp(gdp_year: int, weo_year: int, weo_release: int) -> dict:
    """
    Retrieves gdp value for a specific year
    """
    # download weo data
    _download_weo(year=weo_year, release=weo_release)

    # Read the weo data
    df = weo.WEO(f"{config.paths.raw_data}/weo_{weo_year}_{weo_release}.csv").df

    # Clean the weo data. Filter for GDP, convert to USD. Return as dictionary
    return (
        df.pipe(_clean_weo)
        .loc[
            lambda d: (d.indicator == "NGDPD") & (d.year == gdp_year),
            ["iso_code", "value"],
        ]
        .assign(gdp=lambda d: d.value * 1e9)
        .set_index("iso_code")["gdp"]
        .to_dict()
    )


def add_gdp(df: pd.DataFrame, iso_codes_col: str = "iso_code",) -> pd.DataFrame:
    """adds gdp to a dataframe"""
    gdp: dict = get_gdp(gdp_year=GDP_YEAR, weo_year=WEO_YEAR, weo_release=WEO_RELEASE)

    return df.assign(gdp=lambda d: d[iso_codes_col].map(gdp))
