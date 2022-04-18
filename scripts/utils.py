""" """

import pandas as pd
import wbgapi as wb
from weo import all_releases, WEO


# =============================================================================
#  population
# =============================================================================




def get_wb_indicator(indicator:str):

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

    return (df
            [indicator]
            .astype('int32')
            .to_dict())

def add_population(df):

    pop = get_wb_indicator('SP.POP.TOTL').pipe(wb_indicator_to_dict, 'SP.POP.TOTL')

    df['population'] = df.iso_code.map(pop)

    return df

# =============================================================================
#  gdp
# =============================================================================

#need to add gdp column


