"""
manually download FAO data from https://www.fao.org/faostat/en/#data/RFB
save in raw_data folder as 'fertilizer_fao.csv'

calculated values are averages of 2019 and 2018

"""
import country_converter as coco
import pandas as pd
from scripts.config import paths
from scripts.trade_impact import (
    get_latest_prices_data,
    get_yearly_prices_data,
    get_spending,
    _calc_mean_prices,
)


# fertilisers keys = pink sheet name, values = FAO name
fertilisers = {
    "Phosphate rock": "Phosphate rock",
    "Diammonium phosphate (DAP)": "DAP",
    "Superphosphates above 35%": "TSP",
    "Urea": "Urea ",
    "Potassium chloride (muriate of potash) (MOP)": "Potassium chloride",
}


def clean_raw_fao(df: pd.DataFrame) -> pd.DataFrame:
    """"""

    df = (
        df[["Area", "Element", "Item", "Year", "Unit", "Value"]]
        .pivot(
            index=["Area", "Item", "Unit", "Element"], columns=["Year"], values="Value"
        )
        .reset_index()
    )
    df["value"] = (df[2018] + df[2019]) / 2  # calculate mean of 2019 and 2020
    df = (
        df.drop(columns=[2018, 2019])
        .pivot(index=["Area", "Item"], columns="Element", values="value")
        .reset_index()
        .rename(
            columns={
                "Area": "country",
                "Item": "fertiliser",
                "Agricultural Use": "ag_use",
                "Export Quantity": "export_quantity",
                "Import Quantity": "import_quantity",
                "Export Value": "export_value",
                "Import Value": "import_value",
                "Production": "production",
            }
        )
        .replace({"China, Taiwan Province of": "Taiwan", "China, mainland": "China"})
    )  # fix naming convention for China for country_converter mapping

    # clean countries
    df["iso_code"] = coco.convert(df.country)
    df["continent"] = coco.convert(df.iso_code, to="continent")
    df.country = coco.convert(df.country, to="name_short")

    return df[df.fertiliser.isin(fertilisers.keys())]


def fertilizer_pipeline():
    df = pd.read_csv(f"{paths.raw_data}/fertilizer_fao.csv")
    df = clean_raw_fao(df)
    df["pink_sheet_commodity"] = df.fertiliser.map(fertilisers)
    df["net_import_qty"] = (
        df.import_quantity - df.export_quantity
    )  ################## what about NAN values? Assume 0?
    df["ratio_in_ag"] = df.ag_use / df.import_quantity

    latest_price = get_latest_prices_data(list(fertilisers.values()))
    mean_prices = get_yearly_prices_data(list(fertilisers.values()))
    means = _calc_mean_prices(mean_prices, 2018, 2019)

    df = df.pipe(
        get_spending,
        ["net_import_qty"],
        [("latest", latest_price), ("pre_crisis", means)],
    )

    return df


def flourish_slope_africa(df: pd.DataFrame, fertiliser: str):

    return df[(df.fertiliser == fertiliser) & (df.continent == "Africa")].dropna(
        subset=["value_net_import_qty_latest", "value_net_import_qty_pre_crisis"]
    )[
        [
            "iso_code",
            "country",
            "fertiliser",
            "pink_sheet_commodity",
            "value_net_import_qty_latest",
            "value_net_import_qty_pre_crisis",
        ]
    ]


if __name__ == "__main__":

    data = fertilizer_pipeline()
    # dap = flourish_slope_africa(data, 'Diammonium phosphate (DAP)')
    # potash = flourish_slope_africa(data, 'Potassium chloride (muriate of potash) (MOP)')
    # tsp = flourish_slope_africa(data, 'Superphosphates above 35%')
    # urea = flourish_slope_africa(data, 'Urea')
    # phosphate_rock = flourish_slope_africa(data, 'Phosphate rock')
