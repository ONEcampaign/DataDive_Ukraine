"""
manually download FAO data from https://www.fao.org/faostat/en/#data/RFB
save in raw_data folder as 'fertilizer_fao.csv'

calculated values are averages of 2019 and 2018

"""
import country_converter as coco
from scripts import config
import pandas as pd
from scripts.config import paths
from scripts.trade_impact import (
    get_latest_prices_data,
    get_yearly_prices_data,
    get_spending,
    _calc_mean_prices,
)
from scripts.commodities_analysis import get_commodity_prices


# fertilisers keys = pink sheet name, values = FAO name
fertilisers = {
    "Phosphate rock": "Phosphate rock",
    "Diammonium phosphate (DAP)": "DAP",
    "Superphosphates above 35%": "TSP",
    "Urea": "Urea ",
    "Potassium chloride (muriate of potash) (MOP)": "Potassium chloride",
}


def fertilizer_price_chart() ->None:
    """create csv for fertilizer price chart from WB pink sheet"""

    df = get_commodity_prices(list(fertilisers.values()))
    df = df[df.period>='2007-05-01'].melt('period').reset_index(drop=True)
    df.columns = ['period', 'fertilizer', 'price']

    df.to_csv(f'{config.paths.output}/fertiliser_prices.csv', index=False)





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
    df.import_quantity = df.import_quantity.fillna(0)
    df.export_quantity = df.export_quantity.fillna(0)
    df["net_import_qty"] = (
        df.import_quantity - df.export_quantity
    )
    #df["ratio_in_ag"] = df.ag_use / df.import_quantity

    latest_price = get_latest_prices_data(list(fertilisers.values()))
    mean_prices = get_yearly_prices_data(list(fertilisers.values()))
    means = _calc_mean_prices(mean_prices, 2018, 2019)

    df = df.pipe(
        get_spending,
        ["net_import_qty"],
        [("latest", latest_price), ("pre_crisis", means)],
    )
    df['change'] = df.value_net_import_qty_latest - df.value_net_import_qty_pre_crisis

    return df


def flourish_slope_africa(df: pd.DataFrame, fertiliser_list: list):

    return df.loc[(df.fertiliser.isin(fertiliser_list))
              & (df.continent == "Africa")
              &(df.net_import_qty>=0)
              &(df.change!=0),
                  [
                      "iso_code",
                      "country",
                      "fertiliser",
                      "pink_sheet_commodity",
                      "value_net_import_qty_latest",
                      "value_net_import_qty_pre_crisis",
                  ]].dropna(
        subset=["value_net_import_qty_latest", "value_net_import_qty_pre_crisis"]
    )


if __name__ == "__main__":

    data = fertilizer_pipeline()
    
