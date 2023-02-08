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


def update_wb_indicator(id_: str) -> None:
    # get population data
    get_wb_indicator(id_).to_csv(config.paths.raw_data + rf"/{id_}.csv", index=True)


def add_population(df: pd.DataFrame, iso_codes_col: str = "iso_code") -> pd.DataFrame:
    """Adds population to a dataframe"""

    id_ = "SP.POP.TOTL"
    pop_: dict = pd.read_csv(
        config.paths.raw_data + rf"/{id_}.csv", dtype={f"{id_}": float}, index_col=0
    ).pipe(wb_indicator_to_dict, id_)

    return df.assign(population=lambda d: d[iso_codes_col].map(pop_))


def add_health_pc(df: pd.DataFrame, iso_codes_col: str = "iso_code") -> pd.DataFrame:
    """Adds population to a dataframe"""

    id_ = "SH.XPD.GHED.PC.CD"
    pop_: dict = pd.read_csv(
        config.paths.raw_data + rf"/{id_}.csv", dtype={f"{id_}": float}, index_col=0
    ).pipe(wb_indicator_to_dict, id_)

    return df.assign(health_spending=lambda d: d[iso_codes_col].map(pop_))


def get_debt_service(year: int = 2022) -> dict:
    import country_converter as coco

    df = pd.read_csv(config.paths.raw_data + r"/ids_service_raw.csv")

    df = (
        df.loc[lambda d: (d["counterpart-area"] == "World") & (d.time == year)]
        .groupby(["country", "time"], as_index=False)["value"]
        .sum()
        .assign(iso_code=lambda d: coco.convert(d.country, to="ISO3"))
        .loc[lambda d: d.iso_code != "not found"]
        .reset_index(drop=True)
    )
    return df.set_index("iso_code").value.to_dict()


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


def get_gdp(gdp_year: int) -> dict:
    """
    Retrieves gdp value for a specific year
    """

    # Read the weo data
    df = weo.WEO(f"{config.paths.raw_data}/weo_{WEO_YEAR}_{WEO_RELEASE}.csv").df

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


def add_gdp(
    df: pd.DataFrame,
    iso_codes_col: str = "iso_code",
) -> pd.DataFrame:
    """adds gdp to a dataframe"""
    gdp: dict = get_gdp(gdp_year=GDP_YEAR)

    return df.assign(gdp=lambda d: d[iso_codes_col].map(gdp))


# =============================================================================
#  income levels
# =============================================================================


def _download_income_levels():
    """Downloads fresh version of income levels from WB"""
    url = "https://databank.worldbank.org/data/download/site-content/CLASS.xlsx"

    df = pd.read_excel(
        url,
        sheet_name="List of economies",
        usecols=["Code", "Income group"],
        na_values=None,
    )

    df = df.dropna(subset=["Income group"])

    df.to_csv(config.paths.raw_data + r"/income_levels.csv", index=False)
    print("Downloaded income levels")


def get_income_levels() -> dict:
    """Return income level dictionary"""
    file = config.paths.raw_data + r"/income_levels.csv"
    return pd.read_csv(file, na_values=None, index_col="Code")["Income group"].to_dict()


def add_income_levels(
    df: pd.DataFrame, iso_codes_col: str = "iso_code"
) -> pd.DataFrame:
    """Add income levels to a dataframe"""
    income_levels: dict = get_income_levels()

    return df.assign(income_level=lambda d: d[iso_codes_col].map(income_levels))


# =============================================================================
#  PPP conversion
# =============================================================================


def add_ppp(
    df: pd.DataFrame, iso_codes_col: str = "iso_code", usd_values_col: str = "value"
) -> pd.DataFrame:
    """Adds PPP values to a dataframe"""

    # GDP conversion factor for LCU to International USD (PPP)
    lcu_ppp_id = "PA.NUS.PPP"
    lcu_ppp: dict = pd.read_csv(config.paths.raw_data + rf"/{lcu_ppp_id}.csv").pipe(
        wb_indicator_to_dict, lcu_ppp_id
    )

    # Official exchange rate (LCU per US$, period average)
    lcu_usd_id = "PA.NUS.FCRF"
    lcu_usd: dict = pd.read_csv(config.paths.raw_data + rf"/{lcu_usd_id}.csv").pipe(
        wb_indicator_to_dict, lcu_usd_id
    )

    df[f"{usd_values_col}_ppp"] = (
        df[usd_values_col] * df[iso_codes_col].map(lcu_usd)
    ) / df[iso_codes_col].map(lcu_ppp)

    return df


if __name__ == "__main__":
    _download_income_levels()
    _download_weo(year=WEO_YEAR, release=WEO_RELEASE)
    update_wb_indicator(id_="SP.POP.TOTL")
    update_wb_indicator(id_="PA.NUS.PPP")
    update_wb_indicator(id_="PA.NUS.FCRF")

    # Education
    update_wb_indicator(id_="SE.XPD.TOTL.GD.ZS")

    # Health per capita
    update_wb_indicator(id_="SH.XPD.GHED.PC.CD")
