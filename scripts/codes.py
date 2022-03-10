import pandas as pd
from scripts.config import paths


def cereals_dict(commodity_codes: pd.array) -> dict[str:str]:
    cereals = {
        "100111": "Wheat",
        "100119": "Wheat",
        "100191": "Wheat",
        "100199": "Wheat",
        "100310": "Barley",
        "100390": "Barley",
        "100510": "Maize",
        "100590": "Maize",
    }

    for k in commodity_codes:
        if k in range(100111, 100900) and k not in cereals:
            cereals[str(k)] = "Other cereals"

    return cereals


def fuels_dict(commodity_codes: pd.array) -> dict[str:str]:
    fuels = {
        "270111": "Coal",
        "270112": "Coal",
        "270119": "Coal",
        "270900": "Petroleum oils",
        "271000": "Petroleum oils",
        "271111": "Gas",
        "271112": "Gas",
        "271113": "Gas",
        "271114": "Gas",
        "271119": "Gas",
        "271121": "Gas",
        "271129": "Gas",
    }

    for k in commodity_codes:
        if k in range(270000, 280000) and k not in fuels:
            fuels[str(k)] = "Other fuels"

    return fuels


def vegetable_oils_dict(commodity_codes: pd.array) -> dict[str:str]:
    oils = {
        "151211": "Sunflower oil",
        "151219": "Sunflower oil",
    }

    for k in commodity_codes:
        if k in range(150000, 160000) and k not in oils:
            oils[str(k)] = "Other oils and fats"

    return oils


def steel_dict() -> dict[str:str]:
    return (pd.read_csv(
        paths.raw_data + "/product_codes.csv", dtype={"code": int, "bec": str}
    )

    .loc[lambda d: d.code.isin(range(720000, 730000))]
    .astype({'code': str})
    .set_index("code")
    .assign(description='Steel and Iron')
    ["description"]
    .to_dict()
            )

def weapons_dict() -> dict[str:str]:
    return (pd.read_csv(
        paths.raw_data + "/product_codes.csv", dtype={"code": int, "bec": str}
    )

            .loc[lambda d: d.code.isin(range(930000, 940000))]
            .astype({'code': str})
            .set_index("code")
            .assign(description='Weapons, Firearms, Ammunition')
            ["description"]
            .to_dict()
            )

def potash_dict(commodity_codes: pd.array) -> dict[str:str]:
    potash = {

        "310420": "Potash",
        "310430": "Potash",
        "310490": "Potash",
        "281520": "Potash",
        "310520": "Potash",
        "252910": "Potash",

    }

    #for k in commodity_codes:
    #    if k in range(270000, 280000) and k not in potash:
    #        potash[str(k)] = "Other fuels"

    return potash


def bec_dict() -> dict[str:str]:
    return (
        pd.read_csv(
            paths.raw_data + "/hs17_bec.csv", dtype={"HS 2017": str, "BEC": str}
        )
        .set_index("HS 2017")["BEC"]
        .to_dict()
    )


def codes_dict_bec() -> dict[str:str]:
    return (
        pd.read_csv(
            paths.raw_data + "/product_codes.csv", dtype={"code": str, "bec": str}
        )
        .set_index("code")["bec"]
        .to_dict()
    )


def bec_names() -> dict[str:str]:
    return {
        "1": "Food and beverages",
        "11": "Food and beverages",
        "111": "Food and beverages",
        "112": "Food and beverages",
        "12": "Food and beverages",
        "121": "Food and beverages",
        "122": "Food and beverages",
        "2": "Industrial Supplies",
        "21": "Industrial Supplies",
        "22": "Industrial Supplies",
        "3": "Fuels and Lubricants",
        "31": "Fuels and Lubricants",
        "32": "Fuels and Lubricants",
        "321": "Fuels and Lubricants",
        "322": "Fuels and Lubricants",
        "4": "Capital goods",
        "41": "Capital goods ",
        "42": "Capital goods",
        "5": "Transport equipment",
        "51": "Transport equipment",
        "52": "Transport equipment",
        "521": "Transport equipment",
        "522": "Transport equipment",
        "53": "Transport equipment",
        "6": "Consumption goods n.e.s",
        "61": "Consumption goods n.e.s",
        "62": "Consumption goods n.e.s",
        "63": "Consumption goods n.e.s",
        "7": "Goods not specified",
    }
