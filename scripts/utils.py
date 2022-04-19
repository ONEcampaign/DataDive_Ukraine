""" """

import pandas as pd
import wbgapi as wb
import weo
from scripts import config


# =============================================================================
#  population
# =============================================================================

def get_wb_indicator(indicator:str):
    """query wb api"""
    try:
        return wb.data.DataFrame(indicator ,
                                 mrnev = 1,
                                 numericTimeKeys=True,
                                 labels = False,
                                 columns = 'series',
                                 timeColumns=True)
    except ConnectionError:
        raise ConnectionError(f"Could not retrieve indicator {indicator}")

def wb_indicator_to_dict(df, indicator):
    """converts a wb dataframe to a dictionary where keys are iso3 codes and values are indicator values"""
    return (df
            [indicator]
            .astype('int32')
            .to_dict())

def add_population(df):
    "adds population to a dataframe"
    pop = get_wb_indicator('SP.POP.TOTL').pipe(wb_indicator_to_dict, 'SP.POP.TOTL')

    df['population'] = df.iso_code.map(pop)

    return df

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

    columns = names = {
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
    df = (
        df.drop(cols_to_drop, axis=1)
            .rename(columns=columns)
            .melt(id_vars=names.values(), var_name="year", value_name="value")
    )
    df.value = df.value.map(
        lambda x: (str(x).strip().replace(",", "").replace("--", ""))
    )
    df.year = pd.to_numeric(df.year)
    df.value = pd.to_numeric(df.value, errors="coerce")

    return df

def get_gdp(gdp_year: int, weo_year: int, weo_release: int) -> dict:
    """
    Retrieves gdp value for a specific year
    """

    _download_weo(year=weo_year, release=weo_release)
    df = weo.WEO(f"{config.paths.raw_data}/weo_{weo_year}_{weo_release}.csv").df
    df = _clean_weo(df)
    df = df[(df.indicator == "NGDPD") & (df.year == gdp_year)][["iso_code", "value"]]
    df.value = df.value * 1e9
    df.rename(columns={"value": "gdp"}, inplace=True)

    return df.set_index('iso_code')['gdp'].to_dict()


def add_gdp(df):
    """adds gdp to a dataframe"""
    gdp = get_gdp(2021, 2021, 2)
    df['gdp'] = df.iso_code.map(gdp)

    return df