library(rlang)
library(readr)
library(dplyr)
library(tidyr)
library(stringr)
library(purrr)
library(tibble)
library(readxl)
library(stringi)
library(lubridate)
library(janitor)
library(rjson)
library(xml2)
library(rvest)
library(zoo)
library(magrittr)
library(googlesheets4)
library(httr)

# Socrata password
SCT_PW <- Sys.getenv("SCT_PW")

# Latest week of analysis date
weekOfAnalysisDate <- list.files("./visualizations/") %>% 
  str_match_all("\\d{4}-\\d{2}-\\d{2}_.*") %>%
  compact() %>% 
  str_extract_all("\\d{4}-\\d{2}-\\d{2}") %>% 
  flatten_chr() %>% 
  base::as.Date() %>% 
  sort(decreasing = T) %>% 
  nth(1) + 7

### Configure googlesheet4 auth

paste0("{
\"type\": \"service_account\",
  \"project_id\": \"inv-ny1-covid-recovery-index\",
  \"private_key_id\": \"", Sys.getenv("PRIVATE_KEY_ID"), "\",
  \"private_key\": \"", Sys.getenv("PRIVATE_KEY"), "\",
  \"client_email\": \"", Sys.getenv("CLIENT_EMAIL"), "\",
  \"client_id\": \"", Sys.getenv("PRIVATE_KEY_ID"), "\",
  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",
  \"token_uri\": \"https://oauth2.googleapis.com/token\",
  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",
  \"client_x509_cert_url\": \"", Sys.getenv("CLIENT_CERT_URL"), "\"
}") -> google_service_account


gs4_auth(path = google_service_account)

### Open Table Data
## From section: https://www.opentable.com/state-of-industry
## Seated diners from online, phone, and walk-in reservations

Sys.sleep(3)
otUpdate <- tryCatch({

  otScripts <- read_html("https://www.opentable.com/state-of-industry") %>%
    html_nodes("script")

  otDataScript <- which(map_lgl(map_chr(otScripts, html_text), ~str_detect(.x, "__INITIAL_STATE__")))

  otRawText <- otScripts %>%
    nth(otDataScript) %>%
    html_text()

  Sys.sleep(5)

  otRawJSON <- otRawText %>%
    str_match(regex("w.__INITIAL_STATE__ =\\s+(.*)", dotall = T)) %>%
    nth(2) %>%
    str_remove(";\\}\\)\\(window\\);$")


  Sys.sleep(5)

  otData <- otRawJSON %>%
    fromJSON(json_str = .)


  otChrDates <- otData %>%
    extract2("covidDataCenter") %>%
    extract2("fullbook") %>%
    extract2("headers")

  otDates <- lubridate::ymd(otChrDates) %>% sort()

  ot_change_all_no_date <- otData %>%
    extract2("covidDataCenter") %>%
    extract2("fullbook") %>%
    extract2("cities") %>%
    map_dfr(., ~tibble(
      pct_chg = .$yoy,
      city = .$name
    )) %>%
    filter(city %in% c("New York", "Los Angeles", "Chicago", "Houston", "Washington"))
  
  ot_change_all_no_date %>% 
    mutate(date = rep(otDates, 5)) -> ot_change_all
  
  ot_change_all %>% 
    pivot_wider(names_from = city, values_from = pct_chg) %>% 
    mutate(across(!date, function(x) rollmean(x, k = 7, fill = NA, align = "right"))) %>% 
    filter(date <= weekOfAnalysisDate) %>% 
    arrange(desc(date)) %>% 
    write_csv("./data/deep_dive_data/opentable_city_comparison.csv")
  
  ot_change_all %>% 
    filter(city == "New York") -> ot_change_nyc

  otOld <- read_csv("./data/opentable.csv",
                    col_types = "iDdddd")

  openTableReadyNew <- ot_change_nyc %>%
    mutate(Date = otDates, prePanChg = rollmean(pct_chg, k = 7, fill = NA, align = "right")) %>%
    select(-pct_chg) %>%
    pivot_wider(names_from = "city", values_from = "prePanChg") %>%
    rename(`7-day Average` = `New York`) %>%
    mutate(`Day of Week` = wday(Date),
           `OpenTable YoY Seated Diner Data (%)` = NA_integer_,
           `Restaurant Reservations Index Original` = `7-day Average` + 100,
           `Restaurant Reservations Index` = if_else(`Restaurant Reservations Index Original` > 100,
                                                     100,
                                                     `Restaurant Reservations Index Original`)) %>%
    relocate(`Day of Week`, .before = Date) %>%
    filter(Date > max(otOld$Date) & Date <= weekOfAnalysisDate) %>%
    arrange(desc(Date))

  openTableReady <- bind_rows(openTableReadyNew, otOld)

},
error = function(e) {
  eFull <- error_cnd(class = "openTableError", message = paste("An error occured with the Open Table update:",
                                                               e, "on", Sys.Date(), "\n"))

  write(eFull[["message"]], "./errorLog.txt", append = T)
  message(eFull[["message"]])

  return(eFull)
}
)

Sys.sleep(3)

### MTA ridership
Sys.sleep(3)
mtaUpdate <- tryCatch(
  {
    
    mta_rows <- as.integer(Sys.Date()) - as.integer(base::as.Date("2020-03-01"))
    
    mta_res <- GET(url = paste0("https://data.ny.gov/resource/vxuj-8kew.csv?$limit=", 
                                mta_rows),
               authenticate(user = "anesta@dotdash.com", password = SCT_PW))
    
    stop_for_status(mta_res)
    
    mta_full_raw <- content(mta_res, encoding = "UTF-8", 
                            col_types = cols(.default = col_character()))
    
    mta_full_raw %>% 
      mutate(date = base::as.Date(date), 
             across(!date, function(x) rollmean(as.double(x), k = 7, fill = NA, align = "left"))) %>% 
      write_csv("./data/deep_dive_data/mta_transit_method_comparison.csv")
    
    mta_full_raw %>% 
      select(date, subways_total_estimated, subways_of_comparable_pre) %>% 
      mutate(date = base::as.Date(date),
             across(starts_with("subways"), as.double),
             subways_of_comparable_pre = subways_of_comparable_pre - 1,
             `7-day Average` = rollmean(subways_of_comparable_pre, k = 7, fill = NA, align = "left"),
             `Subway Mobility Index Original` = (1 + `7-day Average`) * 100,
             `Subway Mobility Index` = if_else(`Subway Mobility Index Original` > 100, 100, `Subway Mobility Index Original`),
             `Day of Week` = c(7, rep_len(seq(1, 7, 1), nrow(.) - 1)),
             `Avg. Ridership` = rollmean(subways_total_estimated, k = 7, fill = NA, align = "left")) %>% 
      relocate(`Day of Week`, .before = date) %>%
      relocate(subways_total_estimated, .after = last_col()) %>% 
      arrange(desc(date)) %>% 
      filter(date <= weekOfAnalysisDate) -> mtaRidershipNYC

  },
  error = function(e) {
    eFull <- error_cnd(class = "mtaError", message = paste("An error occured with the MTA update:",
                                                           e, "on", Sys.Date(), "\n"))

    write(eFull[["message"]], "./errorLog.txt", append = T)
    message(eFull[["message"]])

    return(eFull)
  }
)


Sys.sleep(3)
### UI Data
uiUpdate <- tryCatch({


  ### Citywide aggregated from Weekly County Claims
  download.file(url = "https://dol.ny.gov/statistics-weekly-claims-and-benefits-report",
                destfile = "./data/nyUIClaimsWeekly.xlsx",
                method = "libcurl")

  read_excel(path = "./data/nyUIClaimsWeekly.xlsx",
             sheet = "Weekly Initial Claims by County",
             skip = 1) %>%
    pivot_longer(!c(Region, County), names_to = "Date", values_to = "Claims") %>%
    rename_with(.fn = make_clean_names, .cols = everything()) %>%
    mutate(date = base::as.Date(date, format = "%B %d, %Y"),
           claims = as.integer(claims),
           isoweek = isoweek(date)) %>%
    group_by(region, date, isoweek) %>%
    summarize(total_claims = sum(claims, na.rm = T)) %>%
    ungroup() %>%
    filter(region == "New York City", date >= base::as.Date("2020-01-01")) -> latest_claims


  read_csv("./data/ui_claims_2019.csv", col_names = T,
           col_types = "Diid") -> claims2019

  latest_claims %>%
    left_join(claims2019, by = "isoweek") %>%
    mutate(pct_change_19_now = (total_claims - rolling_avg_claims) / rolling_avg_claims,
           `Unemployment Claims Index Original` = 100 / ((100 * pct_change_19_now) + 100) * 100,
           `Unemployment Claims Index` = if_else(`Unemployment Claims Index Original` > 100, 
                                                 100, 
                                                 `Unemployment Claims Index Original`)) %>%
    arrange(desc(date)) -> updatedNYCUI
  
  # Deep dive analysis
  ## Industry and Occupation Group
  walk2(list("Monthly IC by Industry", "Monthly IC by Occ Group"), 
        list("Industry", "Occupational Group"),
        function(x, y) {
          col_name <- sym(y)
          
          df <- read_excel(path = "./data/nyUIClaimsWeekly.xlsx",
                     sheet = x,
                     skip = 1) %>% 
            filter(Region == "New York City") %>% 
            pivot_longer(!c(Region, !!col_name), names_to = "Month", values_to = "Claims") %>% 
            mutate(Month = base::as.Date(paste(Month, "01"), "%B %Y %d")) %>% 
            group_by(Month) %>% 
            mutate(Total_Claims = sum(Claims, na.rm = T)) %>% 
            ungroup() %>% 
            mutate(Prop_Claims = round((Claims / Total_Claims) * 100, 2)) %>% 
            select(-c(Claims, Total_Claims)) %>% 
            pivot_wider(names_from = !!col_name, values_from = Prop_Claims)
          
          filename <- str_replace_all(str_to_lower(x), "\\s+", "_")
          
          write_csv(df, paste0("./data/deep_dive_data/", filename, ".csv"))
          
          
        })
  
  # County
  read_excel(path = "./data/nyUIClaimsWeekly.xlsx",
             sheet = "Weekly Initial Claims by County",
             skip = 1) %>%
    pivot_longer(!c(Region, County), names_to = "Date", values_to = "Claims") %>%
    rename_with(.fn = make_clean_names, .cols = everything()) %>%
    mutate(date = base::as.Date(date, format = "%B %d, %Y"),
           claims = as.integer(claims)) %>% 
    filter(region == "New York City") %>% 
    select(!region) %>% 
    group_by(date) %>% 
    mutate(tot_claims = sum(claims, na.rm = T)) %>% 
    ungroup() %>% 
    mutate(prop_claims = round((claims / tot_claims) * 100, 2)) %>% 
    select(-c(claims, tot_claims)) %>% 
    pivot_wider(names_from = county, values_from = prop_claims) %>% 
    write_csv("./data/deep_dive_data/weekly_ic_claims_borough.csv")

},
error = function(e) {
  eFull <- error_cnd(class = "uiError", message = paste("An error occured with the UI update:",
                                                        e, "on", Sys.Date(), "\n"))

  write(eFull[["message"]], "./errorLog.txt", append = T)
  message(eFull[["message"]])

  return(eFull)
})


### Covid-19 Data
Sys.sleep(3)
covidUpdate <- tryCatch({

  newNYCCovid19Hospitalizations <- read_csv("https://raw.githubusercontent.com/nychealth/coronavirus-data/master/latest/now-data-by-day.csv",
                                            col_types = cols(.default = col_character()), col_names = T) %>%
    select(date_of_interest, HOSPITALIZED_COUNT) %>%
    mutate(date_of_interest = mdy(date_of_interest),
           HOSPITALIZED_COUNT = as.integer(HOSPITALIZED_COUNT),
           rolling_seven = rollmean(HOSPITALIZED_COUNT, k = 7, fill = NA, align = "right"),
           log_hosp = log10(rolling_seven + 1),
           `Covid-19 Hospitalizations Index Original` = (1 - log_hosp / 3.5) * 100,
           `Covid-19 Hospitalizations Index` = if_else(`Covid-19 Hospitalizations Index Original` > 100,
                                                       100,
                                                       `Covid-19 Hospitalizations Index Original`)) %>%
    filter(!is.na(`Covid-19 Hospitalizations Index`), date_of_interest <= weekOfAnalysisDate) %>%
    arrange(desc(date_of_interest))

  fullNYCCovid19Hospitalizations <- read_csv("./data/nyc_covid19_hospitalizations.csv",
                                             col_types = "Didddd")

  newNYCCovid19Hospitalizations <- newNYCCovid19Hospitalizations %>%
    filter(date_of_interest > max(fullNYCCovid19Hospitalizations$date_of_interest))

  updatedNYCCovid19Hospitalizations <- bind_rows(newNYCCovid19Hospitalizations, fullNYCCovid19Hospitalizations)
  
  # ICU and ICU intubated
  GET(url = "https://health.data.ny.gov/resource/jw46-jpb7.csv?$limit=500000",
      authenticate(user = "anesta@dotdash.com", password = SCT_PW)) -> nys_covid_res
  
  Sys.sleep(3)
  stop_for_status(nys_covid_res)
  
  content(nys_covid_res, encoding = "UTF-8", col_types = cols(.default = col_character())) -> nys_covid_raw
  
  Sys.sleep(3)
  
  nys_covid_raw %>% 
    filter(ny_forward_region == "NEW YORK CITY") %>% 
    mutate(as_of_date = base::as.Date(as_of_date)) %>% 
    group_by(as_of_date) %>% 
    summarize(patients_currently_in_icu = sum(as.integer(patients_currently_in_icu), na.rm = T),
              patients_currently_icu_intubated = sum(as.integer(patients_currently_icu), na.rm = T)) %>% 
    mutate(across(!as_of_date, function(x) rollmean(as.double(x), k = 7, fill = NA, align = "right"))) %>% 
    arrange(desc(as_of_date)) %>% 
    write_csv("./data/deep_dive_data/nyc_covid_icu_and_intubated.csv")
  

},
error = function(e) {
  eFull <- error_cnd(class = "covidError", message = paste("An error occured with the Covid update:",
                                                           e, "on", Sys.Date(), "\n"))

  write(eFull[["message"]], "./errorLog.txt", append = T)
  message(eFull[["message"]])

  return(eFull)
})

### Home Sales Street Easy
## TODO: Add in borough by borough home sales breakout and change over time
Sys.sleep(3)
homeSalesUpdate <- tryCatch({

  streetEasyLatest <- read_sheet(ss = "17v5PF6LqZLbNEhq2BPKarmukR_hxn7lXXq27weXSXxk",
                                 sheet = "City Wide Data",
                                 col_types = "Diiii") %>%
    mutate(iso_week = isoweek(`Week Ending`)) %>%
    filter(between(`Week Ending`, left = as.Date("2020-01-01"), right = (weekOfAnalysisDate + 1))) %>%
    select(`Week Ending`, `Number of Pending Sales`, iso_week)

  streetEasyRef <- read_csv("./data/streeteasy_home_sales_ref.csv", col_names = T,
                            col_types = "dd")

  streetEasyLatest %>%
    inner_join(streetEasyRef, by = "iso_week") %>%
    mutate(`Home Sales Index Original` = (`Number of Pending Sales` / three_week_2019_rolling_average) * 100,
           `Home Sales Index` = if_else(`Home Sales Index Original` > 100, 100, `Home Sales Index Original`),
           Date = (`Week Ending` - 1)) %>%
    arrange(desc(`Week Ending`)) -> streetEasyFull


},
error = function(e) {
  eFull <- error_cnd(class = "streetEasy", message = paste("An error occured with the streetEasy update:",
                                                           e, "on", Sys.Date(), "\n"))

  write(eFull[["message"]], "./errorLog.txt", append = T)
  message(eFull[["message"]])

  return(eFull)
})

Sys.sleep(5)
## TODO: Add in borough by borough rental vacancy change over time
rentalsUpdate <- tryCatch({

  nycRentalsFull <- read_csv("./data/streeteasy_rentals.csv",
                             col_types = "iDdidddd")

  latestNYCRental <- read_sheet(ss = "17v5PF6LqZLbNEhq2BPKarmukR_hxn7lXXq27weXSXxk",
                                sheet = "City Wide Data",
                                col_types = "Diiii") %>%
    filter(`Week Ending` == weekOfAnalysisDate + 1) %>%
    select(`Rental Inventory`) %>%
    pull()

  newNYCRentals <- tibble_row(
    `Week of Year` = isoweek(weekOfAnalysisDate),
    `Week Ending` = weekOfAnalysisDate,
    `10-yr Median Model` = case_when(month(weekOfAnalysisDate) == 1 ~ 0.9891,
                                     month(weekOfAnalysisDate) == 2 ~ 0.9811,
                                     month(weekOfAnalysisDate) == 3 ~ 1.0659,
                                     month(weekOfAnalysisDate) == 4 ~ 1.0775,
                                     month(weekOfAnalysisDate) == 5 ~ 1.1359,
                                     month(weekOfAnalysisDate) == 6 ~ 1.1893,
                                     month(weekOfAnalysisDate) == 7 ~ 1.2042,
                                     month(weekOfAnalysisDate) == 8 ~ 1.1728,
                                     month(weekOfAnalysisDate) == 9 ~ 1.0496,
                                     month(weekOfAnalysisDate) == 10 ~ 1.0406,
                                     month(weekOfAnalysisDate) == 11 ~ 0.9774,
                                     month(weekOfAnalysisDate) == 12 ~ 0.8810),
    `Rental Inventory` = latestNYCRental,
    `Indexed to January Average` = (latestNYCRental - latestNYCRental * (1.11108297 - 1) * isoweek(weekOfAnalysisDate) / 52.28571429) / 16366.8,
    Difference = abs(`Indexed to January Average`/`10-yr Median Model`-1),
    `Rental Inventory Index Original` = 100 / (Difference + 1),
    `Rental Inventory Index` = if_else(`Rental Inventory Index Original` > 100, 
                                       100, 
                                       `Rental Inventory Index Original`)
  )

  nycRentalsUpdated <- bind_rows(newNYCRentals, nycRentalsFull)

},
error = function(e) {
  eFull <- error_cnd(class = "streetEasy", message = paste("An error occured with the streetEasy update:",
                                                           e, "on", Sys.Date(), "\n"))

  write(eFull[["message"]], "./errorLog.txt", append = T)
  message(eFull[["message"]])

  return(eFull)
})

Sys.sleep(3)
dataFileUpdate <- tryCatch({
  latestWeeks <- streetEasyFull %>%
    inner_join(nycRentalsUpdated, by = c("Date" = "Week Ending")) %>%
    inner_join(updatedNYCCovid19Hospitalizations, by = c("Date" = "date_of_interest")) %>%
    inner_join(updatedNYCUI, by = c("Date" = "date")) %>%
    inner_join(openTableReady, by = "Date") %>%
    inner_join(mtaRidershipNYC, by = c("Date" = "date")) %>%
    select(Date, `Covid-19 Hospitalizations Index`, `Unemployment Claims Index`,
           `Home Sales Index`, `Rental Inventory Index`, `Subway Mobility Index`,
           `Restaurant Reservations Index`) %>%
    filter(Date %in% c(weekOfAnalysisDate, weekOfAnalysisDate - 7)) %>%
    arrange(Date)


  WoWChange <- map_dfr(latestWeeks[2:7], diff) %>%
    mutate(`New York City Recovery Index` = sum(
      map_dbl(
        select(
          filter(
            latestWeeks, Date == weekOfAnalysisDate
          ), -Date
        ), ~.x/6
      )) - sum(
        map_dbl(
          select(
            filter(
              latestWeeks, Date == weekOfAnalysisDate - 7
            ), -Date
          ), ~.x/6
        )
      )
    ) %>%
    relocate(`New York City Recovery Index`, everything()) %>%
    pivot_longer(everything(), names_to = "DATE", values_to = as.character(weekOfAnalysisDate))


  indexRecoveryOverview <- read_csv("./data/nyc_recovery_index_overview.csv",
                                    col_types = "Ddddddd")


  latestNYRIScores <- latestWeeks %>%
    filter(Date == weekOfAnalysisDate) %>%
    mutate(across(2:7, ~. / 6))


  indexRecoveryOverviewLatest <- bind_rows(latestNYRIScores, indexRecoveryOverview)

},
error = function(e) {
  eFull <- error_cnd(class = "dataUpdate", message = paste("An error occured with the data writing update:",
                                                           e, "on", Sys.Date(), "\n"))

  write(eFull[["message"]], "./errorLog.txt", append = T)
  message(eFull[["message"]])

  return(eFull)
})

if (any(map_lgl(list(otUpdate, mtaUpdate, uiUpdate, covidUpdate,
                     homeSalesUpdate, rentalsUpdate, dataFileUpdate
), ~class(.x)[2] == "rlang_error"), na.rm = T)) {
  stop("There was an error in the update, check the error log to see more.")
} else {
  print("Data update was successful! Writing files and pushing to Git...")
  write_csv(openTableReady, "./data/opentable.csv")
  Sys.sleep(2)
  write_csv(mtaRidershipNYC, "./data/mta_subway.csv")
  Sys.sleep(2)
  write_csv(updatedNYCUI, "./data/nyc_ui.csv")
  Sys.sleep(2)
  write_csv(updatedNYCCovid19Hospitalizations, "./data/nyc_covid19_hospitalizations.csv")
  Sys.sleep(2)
  write_csv(streetEasyFull, "./data/streeteasy_home_sales.csv")
  Sys.sleep(2)
  write_csv(nycRentalsUpdated, "./data/streeteasy_rentals.csv")
  Sys.sleep(2)
  write_csv(WoWChange, paste0("./visualizations/", weekOfAnalysisDate, "_DW_NYC_Recovery_Index_WoW_Changes.csv"))
  Sys.sleep(2)
  write_csv(indexRecoveryOverviewLatest, paste0("./visualizations/", weekOfAnalysisDate, "_DW_NYC_Recovery_Index_Overview.csv"))
  Sys.sleep(2)
  write_csv(indexRecoveryOverviewLatest, "./data/nyc_recovery_index_overview.csv")
}

