"""
Functions to assess African countries debt to Ukraine and Russia
"""

from scripts.config import paths
import pandas as pd
import country_converter as coco

CC = coco.CountryConverter()


def africa_dict() -> dict:
    cc = coco.CountryConverter()
    return (
        cc.data.loc[lambda d: d.continent == "Africa"]
        .set_index("ISO3")["continent"]
        .to_dict()
    )


def read_debt_data(indicator: str = "service") -> pd.DataFrame:
    """Read the provided IDS file for either 'service' or 'stocks'"""

    return pd.read_csv(paths.raw_data + fr"/ids_{indicator}_raw.csv")


def filter_creditor_list(
    data: pd.DataFrame, creditor_list: list = None
) -> pd.DataFrame:
    """Filter the data to only include the provided creditor list. Filters Ukraine
    and Russia by default"""

    if creditor_list is None:
        creditor_list = ["Ukraine", "Russia"]

    creditor_list = CC.convert(creditor_list, to="ISO3")

    return (
        data.assign(creditor_iso=lambda d: CC.convert(d["counterpart-area"], to="ISO3"))
        .loc[lambda d: d.creditor_iso.isin(creditor_list)]
        .reset_index(drop=True)
    )


def filter_africa_debtors(data: pd.DataFrame) -> pd.DataFrame:
    """Filter the data to only include African debtors"""

    return (
        data.assign(
            iso_code=lambda d: CC.convert(d.country, to="ISO3"),
            continent=lambda d: d.iso_code.map(africa_dict()),
        )
        .loc[lambda d: d.continent == "Africa"]
        .reset_index(drop=True)
    )


def __simplify(
    data: pd.DataFrame, groups: dict, single_group: dict, to_total: bool
) -> pd.DataFrame:
    """Helper function to simplify debt indicator groupings"""

    columns = [c for c in data.columns if c not in ["series", "series_code"]]

    data = (
        data.assign(indicator=lambda d: d.series.map(groups))
        .groupby(columns + ["indicator"], as_index=False)["value"]
        .sum()
    )

    if not to_total:
        return data
    else:
        return (
            data.assign(indicator=lambda d: d.indicator.map(single_group))
            .groupby(columns + ["indicator"], as_index=False)["value"]
            .sum()
        )


def simplify_service(data: pd.DataFrame, to_total: bool = False) -> pd.DataFrame:
    """Simplify debt service data to group interests and principal,
    and group bilateral and private"""

    groups = {
        "PPG, bilateral (AMT, current US$)": "Bilateral debt service",
        "PPG, bilateral (INT, current US$)": "Bilateral debt service",
        "PPG, commercial banks (AMT, current US$)": "Private debt service",
        "PPG, commercial banks (INT, current US$)": "Private debt service",
        "PPG, other private creditors (AMT, current US$)": "Private debt service",
        "PPG, other private creditors (INT, current US$)": "Private debt service",
    }

    single_group = {
        "Bilateral debt service": "Debt Service",
        "Private debt service": "Debt Service",
    }

    return data.pipe(
        __simplify, groups=groups, single_group=single_group, to_total=to_total
    )


def simplify_stocks(data: pd.DataFrame, to_total: bool = False) -> pd.DataFrame:
    """Simplify debt service data to group interests and principal,
    and group bilateral and private"""

    groups = {
        "PPG, bilateral (DOD, current US$)": "Bilateral debt stock",
        "PPG, commercial banks (DOD, current US$)": "Private debt stock",
        "PPG, other private creditors (DOD, current US$)": "Private debt stock",
    }

    single_group = {
        "Bilateral debt stock": "Debt Stock",
        "Private debt stock": "Debt Stock",
    }

    return data.pipe(
        __simplify, groups=groups, single_group=single_group, to_total=to_total
    )


def debt_pipeline(
    indicator: str = "stocks", start_year: int = 2020, end_year: int = 2021
) -> pd.DataFrame:
    if indicator not in ['stocks', 'service']:
        raise ValueError(f"Invalid indicator: {indicator}")

    if indicator == "stocks":
        return (
            read_debt_data("stocks")
            .pipe(filter_creditor_list)
            .pipe(filter_africa_debtors)
            .pipe(simplify_stocks, to_total=True)
            .loc[lambda d: d.time.isin(range(start_year, end_year + 1))]
            .reset_index(drop=True)
        )
    elif indicator == "service":
        return (
            read_debt_data("service")
            .pipe(filter_creditor_list)
            .pipe(filter_africa_debtors)
            .pipe(simplify_service, to_total=True)
            .loc[lambda d: d.time.isin(range(start_year, end_year + 1))]
            .reset_index(drop=True)
        )


if __name__ == "__main__":

    df = debt_pipeline(indicator="stocks", start_year=2020, end_year=2020)
    df_service = debt_pipeline(indicator="service", start_year=2022, end_year=2022)
