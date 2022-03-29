# NY1 Recovery Index

This repository houses the data and update code for the [New York Recovery Index](https://www.investopedia.com/new-york-city-nyc-economic-recovery-index-5072042) a joint [Investopedia](https://www.investopedia.com/) — [NY1 News](https://www.ny1.com/nyc/all-boroughs) joint data project.

## Methodology

The index consists of six measures of the economic, social, and public health of New York City: COVID-19 hospitalizations, unemployment insurance (UI) claims, MTA subway ridership, restaurant reservations, and housing units available on the rental and purchase market.

Each index measure is compared to a pre-pandemic (typically 2019 level) baseline for said measure in order to calculate how different the latest data measure is from "normal". Indexes are calculated to have scores closer to 0 be farther from normal (i.e. worse) and higher scores be closer to normal (i.e. better). Each individual index measure out of 16 & 2/3rds is the totaled up to see the total index score out of 100. 

Data is gathered and analyzed by the `NY1_Covid_Recovery_Data_Update.R` R file and  CSVs for the [Datawrappers](https://app.datawrapper.de/) are written into the `visualizations` folder with the format `YYYY-MM-DD_Recovery_Index_WoW_Changes.csv` & `YYYY-MM-DD_DW_NYC_Recovery_Index_Overview.csv`. Data for each individual measure are written into CSVs in the `data` folder.

## Data Sources

COVID-19 hospitalization data is taken from the [NYC Department of Health GitHub repository](https://github.com/nychealth/coronavirus-data) and a seven-day trailing average is calculated for the index score.

Unemployment insurance (UI) claims for New York City are calculated from the county level [weekly ui claims report](https://dol.ny.gov/statistics-weekly-claims-and-benefits-report) put out by the [NY Department of Labor](https://dol.ny.gov/labor-data-0). A three-week rolling average of 2019 claims from the counties that make up NYC are also calculated from the same source. Latest UI claims are then compared to claims from the same week in 2019 and the percent change between them is used for the index score.

MTA subway [ridership data]("https://new.mta.info/document/20441) is taken from their [day-by-day ridership page](https://new.mta.info/coronavirus/ridership) and includes the raw total number of estimated daily riderships and the percent of riders measured against comparable pre-pandemic day. Both are used to calculate a seven-day trailing average and the measure index score is derived from the latter.

OpenTable releases [daily data](https://www.opentable.com/state-of-industry) on the percentage change of seated diner restaurant reservations to the comparable day in 2019. This data for New York City is then smoothed via a seven-day trailing average and used to calculate the measure index score.

Housing and rental unit availability on a weekly basis is provided to the Dotdash Meredith team by [StreetEasy](https://streeteasy.com/) a housing and rental market platform in the NYC area. The number of available rental and home units for sale for a given week are then compared to the same week in 2019 for a pre-pandemic change and used to calculate the index measure score. Aggregated monthly data can be found on the [StreetEasy blog](https://streeteasy.com/blog/data-dashboard/).