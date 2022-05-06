"""
manually download FAO data from https://www.fao.org/faostat/en/

fertilizer by product -> save in raw_data folder as 'fertilizer_products_fao.csv'
fertilizer by nutrient group -> save in raw_data folder as fertilizer_nutrient_fao.csv

calculated values are averages of 2019 and 2018

"""
import country_converter as coco
import numpy as np
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
fertiliser_products = {
    "Phosphate rock": "Phosphate rock",
    "Diammonium phosphate (DAP)": "DAP",
    "Superphosphates above 35%": "TSP",
    "Urea": "Urea ",
    "Potassium chloride (muriate of potash) (MOP)": "Potassium chloride",
}

fertilizer_nutrients = ['Nutrient nitrogen N (total)',
                        'Nutrient phosphate P2O5 (total)',
                        'Nutrient potash K2O (total)']
def clean_fao(df:pd.DataFrame) -> pd.DataFrame:
    """Steps to clean FAP dataset"""

    df = (df.loc[df['Year'].isin([2019, 2018, 2017]), ["Area", "Element", "Item", "Year", "Value"]]
          .groupby(["Area", "Element", "Item"]).agg('mean')['Value']
          .reset_index()
          .pivot(index=['Area', 'Item'], columns='Element', values='Value')
          .reset_index()
          .rename(columns={"Area": "country",
                           "Item": "fertiliser",
                           "Agricultural Use": "ag_use",
                           "Export Quantity": "export_quantity",
                           "Import Quantity": "import_quantity",
                           "Export Value": "export_value",
                           "Import Value": "import_value",
                           "Production": "production",})
          .replace({"China, Taiwan Province of": "Taiwan", "China, mainland": "China"})
    )

    # clean countries
    df["iso_code"] = coco.convert(df.country)
    df["continent"] = coco.convert(df.iso_code, to="continent")
    df.country = coco.convert(df.country, to="name_short")

    return df



def _fertilizer_slope(df = pd.DataFrame) -> pd.DataFrame:
    """ """

    df = df.pipe(clean_fao)[df.fertiliser.isin(fertiliser_products.keys())]
    df["pink_sheet_commodity"] = df.fertiliser.map(fertiliser_products)
    df.import_quantity = df.import_quantity.fillna(0)
    df.export_quantity = df.export_quantity.fillna(0)
    df["net_import_qty"] = (
        df.import_quantity - df.export_quantity
    )
    latest_price = get_latest_prices_data(list(fertiliser_products.values()))
    mean_prices = get_yearly_prices_data(list(fertiliser_products.values()))
    means = _calc_mean_prices(mean_prices, 2018, 2019)

    df = df.pipe(
        get_spending,
        ["net_import_qty"],
        [("latest", latest_price), ("pre_crisis", means)],
    )
    df['change'] = df.value_net_import_qty_latest - df.value_net_import_qty_pre_crisis

    return df

def _calculations(df: pd.DataFrame) -> pd.DataFrame:
    """
    calculations for net imports and dependence of net imports in domestic agricultural use
    where there is no domestic use, dependence is set to 0
    """

    df['net_import_quantity'] = df.import_quantity - df.export_quantity

    #adjust net import  - change net export quantity to 0
    df['net_import_quantity_adj'] = df.net_import_quantity
    df.loc[df.net_import_quantity_adj <0, 'net_import_quantity_adj'] = 0

    #net import dependence in agriculture
    df['dependence'] = (df.net_import_quantity_adj/df.ag_use)*100
    df.replace([np.inf, np.nan], 0, inplace=True)
    df.loc[df.dependence > 100, 'dependence'] = 100 #replace values with over 100% dependence with 100

    return df



# =======================================================================
#charts
#========================================================================
def fertilizer_price_chart() ->None:
    """create csv for fertilizer price chart from WB pink sheet"""

    df = get_commodity_prices(list(fertiliser_products.values()))
    df = df[df.period>='2007-05-01'].melt('period').reset_index(drop=True)
    df.columns = ['period', 'fertilizer', 'price']

    df.to_csv(f'{config.paths.output}/fertiliser_prices.csv', index=False)


def flourish_slope_africa(df: pd.DataFrame, fertiliser_list: list, name:str) -> None:
    """creates csv for slope chart"""

    df = (df.pipe(_fertilizer_slope)
        .loc[(df.fertiliser.isin(fertiliser_list))
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
                  ]]
        .dropna(subset=["value_net_import_qty_latest", "value_net_import_qty_pre_crisis"]))

    df.to_csv(f'{config.paths.output}/{name}.csv')



def update_charts() -> None:
    fao_products = pd.read_csv(f"{paths.raw_data}/fertilizer_products_fao.csv")
    fao_nutrients = pd.read_csv(f"{paths.raw_data}/fertilizer_nutrients_fao.csv")


    #slope chart
    # flourish_slope_africa(fao_products,)









if __name__ == "__main__":
    df = pd.read_csv(f"{paths.raw_data}/fertilizer_nutrients_fao.csv")
    df = clean_fao(df)
    df = _calculations(df)