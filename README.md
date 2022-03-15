# Data Dive: The Russian Invasion of Ukraine

This repository powers the analysis for the trade visualizations on the page: 
[Data Dive:The Russian Invasion of Ukraine](https://www.one.org/africa/issues/covid-19-tracker/explore-ukraine/) using bilateral trade from the
[BACI dataset](http://www.cepii.fr/cepii/en/bdd_modele/presentation.asp?id=37) 
and global commodity prices from the [World Bank](https://www.worldbank.org/en/research/commodity-markets#1).


[BACI](http://www.cepii.fr/cepii/en/bdd_modele/presentation.asp?id=37)
provides disaggregated bilateral trade data through official trade data reported on
[COMTRADE](https://comtrade.un.org/). This analysis uses 2017 Harmonized System product nomenclature.
The [World Bank](https://www.worldbank.org/en/research/commodity-markets#1)
provides monthly prices for major global commodities in nominal USD.


## Repository Structure and Information

In order to reproduce the analysis, Python (>=3.10) is needed. Other required packages are
listed in `requirements.txt`. Additionally, bilateral
trade data is too large to be stored in this repository and needs to be manually 
downloaded from [BACI](http://www.cepii.fr/cepii/en/bdd_modele/presentation.asp?id=37).

The repository includes the following sub-folders:
- `output`: contains clean and formatted csv filed that are used to create the visualizations.
- `raw_data`: contains raw data used for the analysis and metadata including product and country
codes. Manually downloaded files are added to this folder.
- `scripts`: scripts for creating the analysis. `codes.py` contains grouped HS codes as lists. 
`read_trade_data.py` contains functions to read BACI trade data from CEPII, do some preprocessing and save the data
as a feather file. `commodities_analysis.py` contains functions to clean and manipulate commodity price data.
`trade_common.py` contains functions to manipulate the trade data and `story.py` creates the final csv files used to produce the flourish visualisations. 
Additionally a `config.py` file manages file paths to different folders.

#### Downloading BACI trade data

The Centre d'Etudes Prospectives et d'Informations Internationales maintains the
[BACI Dataset](http://www.cepii.fr/cepii/en/bdd_modele/presentation.asp?id=37). HS17 data file 
is used for this analysis.

Direct download link: http://www.cepii.fr/DATA_DOWNLOAD/baci/data/BACI_HS17_V202201.zip

Once the zipped file is downloaded the csv filed for 2018, 2019, and 2020 are used. 
These files should be moved to the `raw_data` folder with the naming convention `hs17_{year}.csv"`.








