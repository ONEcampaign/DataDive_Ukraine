"""
Script to create the final csv files used to produce the flourish visualisations
Final csv files are stored in the output folder
"""

import pandas as pd
from scripts.config import paths
from scripts.read_trade_data import world_trade_all_importers, world_trade_africa

from scripts import trade_common as tc
from scripts.debt_analysis import debt_pipeline

# ====================   PARAMETERS  ====================

ALL_IMPORTERS_DATA: pd.DataFrame = world_trade_all_importers()
AFRICA_TRADE_DATA: pd.DataFrame = world_trade_africa()

# =======================================================


def commodity_exports_by_continent(data) -> pd.DataFrame:
    """SLIDES 2 and 3: Major share of commodity exports"""

    return (
        tc.exports_no_intra_europe(data)
        .pipe(tc.summarise_commodity_source_share, "cat2")
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


def exports_to_africa(data: pd.DataFrame) -> pd.DataFrame:
    """Exports to Africa, excluding intra-Africa trade"""
    return (
        tc.exporters_to_africa(data, step_from=0, step_to=1)
        .loc[lambda d: d.source != "Africa"]
        .assign(
            pct=lambda d: round(
                100 * d.value / d.groupby(["year"])["value"].transform("sum"), 2
            )
        )
    )


def russia_ukraine_categories(data: pd.DataFrame) -> pd.DataFrame:
    """Russia and Ukraine exports to categories"""
    return (
        tc.exporter_to_categories(data, 0, 2)
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


def russia_ukraine_categories_country(data: pd.DataFrame) -> pd.DataFrame:
    """Exports to categories broken down by importer country"""
    return (
        tc.exporter_to_categories_by_importer(data, 0, 4)
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


def filter_commodity(data: pd.DataFrame, commodity: str) -> pd.DataFrame:
    """Filter data by commodity and format"""
    return (
        data.loc[lambda d: d.target == commodity]
        .copy()
        .assign(value=lambda d: round(d.value / 1e3, 1))
        .assign(value=lambda d: d.value.astype(str) + "m")
    )


def commodity_bar_explorer(data: pd.DataFrame) -> pd.DataFrame:
    """For a chart to explore commodities by country"""

    return (
        data.assign(value=lambda d: d.value / 1e3)
        .melt(
            id_vars=["year", "importer_name", "target"],
            value_vars=["value", "share"],
            var_name="indicator",
            value_name="value",
        )
        .replace({"indicator": {"value": "USD (million)", "share": "Share"}})
        .pivot_table(
            index=["year", "importer_name", "indicator"],
            columns="target",
            values="value",
        )
    )


def income_levels():
    """Return income level dictionary"""
    file = paths.raw_data + r"/income_levels.csv"
    return pd.read_csv(file, na_values=None, index_col="Code").T.to_dict("records")[0]


def flourish_commodity_explorer() -> None:

    import country_converter as coco

    # Debt
    debt_service = debt_pipeline("service", 2022, 2022)
    debt_stocks = debt_pipeline("stocks", 2020, 2020)

    # UN Vote
    un_vote = pd.read_excel(
        paths.raw_data + r"/resolution_vote.xlsx", sheet_name="detail"
    )

    # Trade
    trade = (
        pd.read_csv(paths.output + r"/rus_ukr_categories_country.csv")
        .assign(
            value=lambda d: d.value / 1e3,
            iso_code=lambda d: coco.convert(d.importer_name, to="ISO3"),
        )
        .pivot_table(index=["iso_code", "year"], columns="target", values="value")
        .reset_index(drop=False)
    )

    explorer = (
        trade.merge(
            un_vote.filter(["vote", "iso_code", "population"]),
            on="iso_code",
            how="left",
        )
        .merge(
            debt_stocks.rename(columns={"value": "Debt Stocks"})
            .groupby("iso_code", as_index=False)["Debt Stocks"]
            .sum(),
            on="iso_code",
            how="outer",
        )
        .merge(
            debt_service.rename(columns={"value": "Debt Service"})
            .groupby("iso_code", as_index=False)["Debt Service"]
            .sum(),
            on="iso_code",
            how="outer",
        )
        .assign(
            income_level=lambda d: d.iso_code.map(income_levels()),
            gdp=lambda d: d.iso_code.map(tc.gdp_dict()),
            name=lambda d: coco.convert(d.iso_code, to="short_name"),
        )
        .dropna(subset=["income_level"])
    )

    explorer.to_clipboard(paths.output + r"/flourish_explorer.csv")


def flourish_story() -> None:
    """Pipeline for the story. This takes the individual steps for the charts and saves
    the results as CSV files"""

    # SLIDES 2 and 3: Major share of key commodity exports
    commodity_exports_share = commodity_exports_by_continent(ALL_IMPORTERS_DATA)
    commodity_exports_share.to_csv(paths.output + r"/commodity_exports_share.csv")

    # Trade with Africa data
    data = tc.exports_to_africa_data(AFRICA_TRADE_DATA)

    # SLIDE 7: Exports to Africa
    exports_slide = exports_to_africa(data)
    exports_slide.to_csv(paths.output + r"/exports_to_africa.csv", index=False)

    # SLIDE 8: Exports to Africa, zoom Ukraine and Russia
    exports_slide_zoom = exports_slide.loc[
        lambda d: d.source.isin(["Ukraine", "Russia"])
    ].copy()
    exports_slide_zoom.to_csv(paths.output + r"/exports_africa_zoom.csv", index=False)

    # SLIDE 9: Ukraine and Russia to African countries
    to_african = tc.exporters_to_african_countries(data, 0, 4).loc[
        lambda d: d.source.isin(["Ukraine", "Russia"])
    ]
    to_african.to_csv(paths.output + r"/ukr_rus_to_african_countries.csv", index=False)

    # SLIDE 12 and 13: Russia and Ukraine to commodity categories
    df_commodity = russia_ukraine_categories(data)
    df_commodity.to_csv(paths.output + r"/rus_ukr_categories.csv", index=False)

    # Russia and Ukraine (together) to African countries by category
    afr_cat = russia_ukraine_categories_country(data)
    afr_cat.to_csv(paths.output + r"/rus_ukr_categories_country.csv", index=False)

    # Explorer
    explore = commodity_bar_explorer(afr_cat)
    explore.to_csv(paths.output + r"/commodity_explore_bar.csv")

    # SLIDE 14 : wheat
    wheat = filter_commodity(afr_cat, "Wheat")
    wheat.to_csv(paths.output + r"/wheat.csv", index=False)
    # SLIDE 15: barley
    barley = filter_commodity(afr_cat, "Barley")
    barley.to_csv(paths.output + r"/barley.csv", index=False)


if __name__ == "__main__":
    flourish_story()
    pass
