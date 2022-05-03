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

### Getting Socrata password from environment variable secret
SCT_PW <- Sys.getenv("SCT_PW")

### Latest week of analysis date by adding seven days to date of latest
### NY1 visualizations.
week_of_analysis_date <- list.files("./visualizations/") %>% 
  str_match_all("\\d{4}-\\d{2}-\\d{2}_.*") %>%
  compact() %>% 
  str_extract_all("\\d{4}-\\d{2}-\\d{2}") %>% 
  flatten_chr() %>% 
  base::as.Date() %>% 
  sort(decreasing = T) %>% 
  nth(1) + days(7)

### Percent Change Function
pct_chg <- function(new, old) {
  round(((new - old) / old) * 100, 2) -> chg
  return(chg)
}

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

# Authenticate googlesheets4
gs4_auth(path = google_service_account)

### Open Table Data
## Source: https://www.opentable.com/state-of-industry
## Seated diners from online, phone, and walk-in reservations

Sys.sleep(3)
ot_update <- tryCatch({

  ot_scripts <- read_html("https://www.opentable.com/state-of-industry") %>%
    html_nodes("script")
  
  # Finding script tag that has the json string data
  ot_data_script <- which(map_lgl(map_chr(ot_scripts, html_text), ~str_detect(.x, "__INITIAL_STATE__")))

  ot_raw_text <- ot_scripts %>%
    nth(ot_data_script) %>%
    html_text()

  Sys.sleep(5)
  
  # Using regex to extract and clean json string data
  ot_raw_json <- ot_raw_text %>%
    str_match(regex("w.__INITIAL_STATE__ =\\s+(.*)", dotall = T)) %>%
    nth(2) %>%
    str_remove(";w.__ENABLE_ANALYTICS_BACKEND__ = true;\\}\\)\\(window\\);$")


  Sys.sleep(5)

  ot_data <- ot_raw_json %>%
    fromJSON(json_str = .)
  

  # Do NOT use the "rollingAverageYYYY" data, calculate trailing seven day average
  # manually from raw day-to-day data in "fullbook"
  ot_chr_dates <- ot_data %>%
    extract2("covidDataCenter") %>%
    extract2("fullbook") %>%
    extract2("headers")

  # Dates need to be extracted this way since some dates (a few holidays and 
  # random other dates) will be missing.
  ot_dates <- lubridate::ymd(ot_chr_dates) %>% sort()

  # Grabbing five of the largest cities in the US to compare to NYC
  ot_change_all_no_date <- ot_data %>%
    extract2("covidDataCenter") %>%
    extract2("fullbook") %>%
    extract2("cities") %>%
    map_dfr(., ~tibble(
      pct_chg = .$yoy,
      city = .$name
    )) %>%
    filter(city %in% c("New York", "Los Angeles", "Chicago", "Houston", "Washington"))
  
  # Adding in data dates
  ot_change_all_no_date %>% 
    mutate(date = rep(ot_dates, 5)) -> ot_change_all
  
  # Calculating trailing seven day average and adding on
  ot_change_all %>% 
    pivot_wider(names_from = city, values_from = pct_chg) %>% 
    mutate(across(!date, function(x) rollmean(x, k = 7, fill = NA, align = "right"))) %>% 
    filter(date <= week_of_analysis_date) %>% 
    arrange(desc(date)) %>% 
    write_csv("./data/deep_dive_data/opentable_city_comparison.csv")
  
  ot_change_all %>% 
    filter(city == "New York") -> ot_change_nyc

  ot_old <- read_csv("./data/opentable.csv",
                    col_types = "iDdddd")

  # Calculating trailing seven day average and restaurant reservations measure index 
  ot_ready_new <- ot_change_nyc %>%
    mutate(date = ot_dates, prePanChg = rollmean(pct_chg, k = 7, fill = NA, align = "right")) %>%
    select(-pct_chg) %>%
    pivot_wider(names_from = "city", values_from = "prePanChg") %>%
    rename(`7-day Average` = `New York`) %>%
    mutate(`Day of Week` = wday(date),
           `OpenTable YoY Seated Diner Data (%)` = NA_integer_,
           `Restaurant Reservations Index Original` = `7-day Average` + 100,
           `Restaurant Reservations Index` = if_else(`Restaurant Reservations Index Original` > 100,
                                                     100,
                                                     `Restaurant Reservations Index Original`)) %>%
    relocate(`Day of Week`, .before = date) %>%
    filter(date > max(ot_old$date) & date <= week_of_analysis_date) %>%
    arrange(desc(date))

  ot_table_ready <- bind_rows(ot_ready_new, ot_old)

},
error = function(e) {
  e_full <- error_cnd(class = "open_table_error", message = paste("An error occured with the OpenTable update:",
                                                               e, "on", Sys.Date(), "\n"))

  message(e_full[["message"]])

  return(e_full)
}
)

Sys.sleep(3)

### MTA ridership
Sys.sleep(3)
mta_update <- tryCatch(
  {
    
    # Getting MTA day-by-day ridership data from the mta data portal via Socrata API
    # https://data.ny.gov/Transportation/MTA-Daily-Ridership-Data-Beginning-2020/vxuj-8kew
    mta_rows <- as.integer(Sys.Date()) - as.integer(base::as.Date("2020-03-01"))
    
    mta_res <- GET(url = paste0("https://data.ny.gov/resource/vxuj-8kew.csv?$limit=", 
                                mta_rows),
               authenticate(user = "anesta@dotdash.com", password = SCT_PW))
    
    stop_for_status(mta_res)
    
    mta_full_raw <- content(mta_res, encoding = "UTF-8", 
                            col_types = cols(.default = col_character()))
    
    # Writing out file comparing trailing seven day averages for each transportation method
    mta_full_raw %>% 
      mutate(date = base::as.Date(date), 
             across(!date, function(x) rollmean(as.double(x), k = 7, fill = NA, align = "left"))) %>% 
      write_csv("./data/deep_dive_data/mta_transit_method_comparison.csv")
    
    # Calculating trailing seven day average of pre-pandemic to current change, raw ridership,
    # and MTA measure index.
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
      filter(date <= week_of_analysis_date) -> mta_ridership_nyc

  },
  error = function(e) {
    e_full <- error_cnd(class = "mta_error", message = paste("An error occured with the MTA update:",
                                                           e, "on", Sys.Date(), "\n"))

    message(e_full[["message"]])

    return(e_full)
  }
)


Sys.sleep(3)
### UI Data
ui_update <- tryCatch({


  # Citywide aggregated from Weekly County Claims
  # From NY DOL: https://dol.ny.gov/labor-data-0 "Weekly UI Claims Report"
  download.file(url = "https://dol.ny.gov/statistics-weekly-claims-and-benefits-report",
                destfile = "./reference_files/ny_ui_claims_weekly.xlsx",
                method = "libcurl")

  read_excel(path = "./reference_files/ny_ui_claims_weekly.xlsx",
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


  read_csv("./reference_files/ui_claims_2019.csv", col_names = T,
           col_types = "Diid") -> claims2019
  
  # Combining latest claims by city with three week rolling average of 2019 claims
  # to create the UI claims measure index
  latest_claims %>%
    left_join(claims2019, by = "isoweek") %>%
    mutate(pct_change_19_now = (total_claims - rolling_avg_claims) / rolling_avg_claims,
           `Unemployment Claims Index Original` = 100 / ((100 * pct_change_19_now) + 100) * 100,
           `Unemployment Claims Index` = if_else(`Unemployment Claims Index Original` > 100, 
                                                 100, 
                                                 `Unemployment Claims Index Original`)) %>%
    arrange(desc(date)) -> updated_nyc_ui
  
  # Deep dive analysis
  ## Industry and Occupation Group
  walk2(list("Monthly IC by Industry", "Monthly IC by Occ Group"), 
        list("Industry", "Occupational Group"),
        function(x, y) {
          col_name <- sym(y)
          
          df <- read_excel(path = "./reference_files/ny_ui_claims_weekly.xlsx",
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
  
  # County (Borough) proportion
  read_excel(path = "./reference_files/ny_ui_claims_weekly.xlsx",
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
  e_full <- error_cnd(class = "ui_error", message = paste("An error occured with the UI update:",
                                                        e, "on", Sys.Date(), "\n"))

  message(e_full[["message"]])

  return(e_full)
})


### Covid-19 Data
Sys.sleep(3)
covid_update <- tryCatch({

  # Fetched from NYC DOH GitHub: https://github.com/nychealth/coronavirus-data
  # Creating trailing seven day averages of all COVID-19 hospitalizations and 
  # calculating COVID-19 measure index.
  new_nyc_covid19_hospitalizations <- read_csv("https://raw.githubusercontent.com/nychealth/coronavirus-data/master/latest/now-data-by-day.csv",
                                            col_types = cols(.default = col_character()), col_names = T) %>%
    select(date_of_interest, HOSPITALIZED_COUNT) %>%
    rename(date = date_of_interest) %>% 
    mutate(date = mdy(date),
           HOSPITALIZED_COUNT = as.integer(HOSPITALIZED_COUNT),
           rolling_seven = rollmean(HOSPITALIZED_COUNT, k = 7, fill = NA, align = "right"),
           log_hosp = log10(rolling_seven + 1),
           `Covid-19 Hospitalizations Index Original` = (1 - log_hosp / 3.5) * 100,
           `Covid-19 Hospitalizations Index` = if_else(`Covid-19 Hospitalizations Index Original` > 100,
                                                       100,
                                                       `Covid-19 Hospitalizations Index Original`)) %>%
    filter(!is.na(`Covid-19 Hospitalizations Index`), date <= week_of_analysis_date) %>%
    arrange(desc(date))

  full_nyc_covid19_hospitalizations <- read_csv("./data/nyc_covid19_hospitalizations.csv",
                                             col_types = "Didddd")

  new_nyc_covid19_hospitalizations <- new_nyc_covid19_hospitalizations %>%
    filter(date > max(full_nyc_covid19_hospitalizations$date))

  updated_nyc_covid19_hospitalizations <- bind_rows(new_nyc_covid19_hospitalizations, full_nyc_covid19_hospitalizations)
  
  # Getting ICU and ICU intubated numbers from the NYS DOH
  # Via Socrata API from health.data.ny.gov 
  # https://health.data.ny.gov/Health/New-York-State-Statewide-COVID-19-Hospitalizations/jw46-jpb7/data
  GET(url = "https://health.data.ny.gov/resource/jw46-jpb7.csv?$limit=500000",
      authenticate(user = "anesta@dotdash.com", password = SCT_PW)) -> nys_covid_res
  
  Sys.sleep(3)
  stop_for_status(nys_covid_res)
  
  content(nys_covid_res, encoding = "UTF-8", col_types = cols(.default = col_character())) -> nys_covid_raw
  
  Sys.sleep(3)
  
  # Writing out trailing seven day averages of nyc icu and icu intubated COVID-19 patients.
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
  e_full <- error_cnd(class = "covid_error", message = paste("An error occured with the Covid update:",
                                                           e, "on", Sys.Date(), "\n"))

  message(e_full[["message"]])

  return(e_full)
})

### Home Sales Street Easy
Sys.sleep(3)
home_sales_update <- tryCatch({
  
  # Reading in latest Streeteasy data 
  streeteasy_latest <- read_sheet(ss = "17v5PF6LqZLbNEhq2BPKarmukR_hxn7lXXq27weXSXxk",
                                 sheet = "City Wide Data",
                                 col_types = "Diiii") %>%
    mutate(iso_week = isoweek(`Week Ending`)) %>%
    filter(between(`Week Ending`, left = as.Date("2020-01-01"), right = (week_of_analysis_date + 1))) %>%
    select(`Week Ending`, `Number of Pending Sales`, iso_week)

  streeteasy_ref <- read_csv("./reference_files/streeteasy_home_sales_ref.csv", col_names = T,
                            col_types = "dd")

  # Calculating latest pending home sales index measure from 2019 3 week
  # rolling average
  streeteasy_latest %>%
    inner_join(streeteasy_ref, by = "iso_week") %>%
    mutate(`Home Sales Index Original` = (`Number of Pending Sales` / three_week_2019_rolling_average) * 100,
           `Home Sales Index` = if_else(`Home Sales Index Original` > 100, 100, `Home Sales Index Original`),
           date = (`Week Ending` - 1)) %>%
    arrange(desc(`Week Ending`)) -> streeteasy_full
  
  # Calculating the WoW, MoM, and YoY changes in pending home sales of streeteasy data
  map_dfr(list("City Wide Data", "Manhattan Data", "Brooklyn Data", "Queens Data"), function(x) {
    
    read_sheet(ss = "17v5PF6LqZLbNEhq2BPKarmukR_hxn7lXXq27weXSXxk",
               sheet = x,
               col_types = "Diiii") -> data
    
    map_dfr(list("WoW", "MoM", "YoY", "19_present"), function(y, df = data) {
      
      if (y == "WoW") {
        days_back <- days(7)
      } else if (y == "MoM") {
        days_back <- days(28)
      } else if (y == "YoY") {
        days_back <- days(364)
      } else {
        days_back <- days(364 * (year(max(df$`Week Ending`)) - 2019))
      }
      
      date_intervals <- df %>% 
        select(c(`Week Ending`, `Number of Pending Sales`)) %>% 
        filter(`Week Ending` %in% c(max(`Week Ending`), max(`Week Ending`) - days_back))
      
      pull(date_intervals[which(date_intervals$`Week Ending` == max(date_intervals$`Week Ending`)), "Number of Pending Sales"]) -> new
      pull(date_intervals[which(date_intervals$`Week Ending` == min(date_intervals$`Week Ending`)), "Number of Pending Sales"]) -> old
      
      Sys.sleep(6)
      
      pct_chg(new, old) -> chg
      tibble(change = y,
             area = x,
             chg = chg) -> latest_row
      
      return(latest_row)

    })
  }) -> borough_pending_sale_changes
  
  write_csv(borough_pending_sale_changes, "./data/deep_dive_data/borough_pending_sale_changes.csv")
  
  # StreetEasy Home Sales By Borough Reference Data
  
  map_dfc(list("City Wide Data", "Manhattan Data", "Brooklyn Data", "Queens Data"), function(x) {
    Sys.sleep(21)
    
    date_col <- str_replace_all(str_to_lower(paste(x, "Week Ending")), "\\s+", "_")
    value_col <- str_replace_all(str_to_lower(paste(x, "Number of Pending Sales")), "\\s+", "_")
    
    read_sheet(ss = "17v5PF6LqZLbNEhq2BPKarmukR_hxn7lXXq27weXSXxk",
               sheet = x,
               col_types = "Diiii") %>% 
      select(c(`Week Ending`, `Number of Pending Sales`)) %>% 
      rename( !!date_col := `Week Ending`,
              !!value_col := `Number of Pending Sales`) -> new_df
    
    return(new_df)
  }) -> home_sales_borough
  
  read_csv("./reference_files/borough_home_sales_ref.csv",
           col_names = T,
           col_types = "idddd") -> borough_home_sales_ref
  
  home_sales_borough %>% 
    mutate(isoweek = isoweek(city_wide_data_week_ending)) %>% 
    left_join(borough_home_sales_ref, by = "isoweek") %>% 
    mutate(city_wide_index = city_wide_data_number_of_pending_sales / nyc_total_2019_rolling_avg * 100,
           man_index = manhattan_data_number_of_pending_sales / man_2019_rolling_avg * 100,
           bk_index = brooklyn_data_number_of_pending_sales / bk_2019_rolling_avg * 100,
           qn_index = queens_data_number_of_pending_sales / qn_2019_rolling_avg * 100) %>% 
    select(isoweek, city_wide_data_week_ending, city_wide_index, man_index, bk_index, qn_index) %>% 
    write_csv("./data/deep_dive_data/borough_home_sales_indexes.csv")
  


},
error = function(e) {
  e_full <- error_cnd(class = "streetEasy", message = paste("An error occured with the streetEasy update:",
                                                           e, "on", Sys.Date(), "\n"))

  message(e_full[["message"]])

  return(e_full)
})

Sys.sleep(5)
rentals_update <- tryCatch({

  # Reading in latest StreetEasy rental vacancy data
  nyc_rentals_full <- read_csv("./data/streeteasy_rentals.csv",
                             col_types = "iDdidddd")

  latest_nyc_rental <- read_sheet(ss = "17v5PF6LqZLbNEhq2BPKarmukR_hxn7lXXq27weXSXxk",
                                sheet = "City Wide Data",
                                col_types = "Diiii") %>%
    filter(`Week Ending` == week_of_analysis_date + 1) %>%
    select(`Rental Inventory`) %>%
    pull()
  
  # Calculating rental goldilocks index measure based off of Amanda Morelli
  # seasonal model from pre-pandemic StreetEasy data
  new_nyc_rentals <- tibble_row(
    `Week of Year` = isoweek(week_of_analysis_date),
    `Week Ending` = week_of_analysis_date,
    `10-yr Median Model` = case_when(month(week_of_analysis_date) == 1 ~ 0.9891,
                                     month(week_of_analysis_date) == 2 ~ 0.9811,
                                     month(week_of_analysis_date) == 3 ~ 1.0659,
                                     month(week_of_analysis_date) == 4 ~ 1.0775,
                                     month(week_of_analysis_date) == 5 ~ 1.1359,
                                     month(week_of_analysis_date) == 6 ~ 1.1893,
                                     month(week_of_analysis_date) == 7 ~ 1.2042,
                                     month(week_of_analysis_date) == 8 ~ 1.1728,
                                     month(week_of_analysis_date) == 9 ~ 1.0496,
                                     month(week_of_analysis_date) == 10 ~ 1.0406,
                                     month(week_of_analysis_date) == 11 ~ 0.9774,
                                     month(week_of_analysis_date) == 12 ~ 0.8810),
    `Rental Inventory` = latest_nyc_rental,
    `Indexed to January Average` = (latest_nyc_rental - latest_nyc_rental * (1.11108297 - 1) * isoweek(week_of_analysis_date) / 52.28571429) / 16366.8,
    Difference = abs(`Indexed to January Average`/`10-yr Median Model`-1),
    `Rental Inventory Index Original` = 100 / (Difference + 1),
    `Rental Inventory Index` = if_else(`Rental Inventory Index Original` > 100, 
                                       100, 
                                       `Rental Inventory Index Original`)
  )

  nycrentals_updated <- bind_rows(new_nyc_rentals, nyc_rentals_full)
  
  # Looking at WoW, MoM, and YoY changes in rental vacancy from StreetEasy data
  map_dfr(list("City Wide Data", "Manhattan Data", "Brooklyn Data", "Queens Data"), function(x) {
    
    read_sheet(ss = "17v5PF6LqZLbNEhq2BPKarmukR_hxn7lXXq27weXSXxk",
               sheet = x,
               col_types = "Diiii") -> data
    
    map_dfr(list("WoW", "MoM", "YoY"), function(y, df = data) {
      
      if (y == "WoW") {
        days_back <- days(7)
      } else if (y == "MoM") {
        days_back <- days(28)
      } else {
        days_back <- days(364)
      } 
      
      date_intervals <- df %>% 
        select(c(`Week Ending`, `Rental Inventory`)) %>% 
        filter(`Week Ending` %in% c(max(`Week Ending`), max(`Week Ending`) - days_back))
      
      pull(date_intervals[which(date_intervals$`Week Ending` == max(date_intervals$`Week Ending`)), "Rental Inventory"]) -> new
      pull(date_intervals[which(date_intervals$`Week Ending` == min(date_intervals$`Week Ending`)), "Rental Inventory"]) -> old
      
      Sys.sleep(6)
      
      pct_chg(new, old) -> chg
      tibble(change = y,
             area = x,
             chg = chg) -> latest_row
      
      return(latest_row)
      
    })
  }) -> borough_rental_changes
  
  write_csv(borough_rental_changes, "./data/deep_dive_data/borough_rental_changes.csv")

},
error = function(e) {
  e_full <- error_cnd(class = "streeteasy", message = paste("An error occured with the streetEasy update:",
                                                           e, "on", Sys.Date(), "\n"))


  message(e_full[["message"]])

  return(e_full)
})

Sys.sleep(3)
data_file_update <- tryCatch({
  
  # Making combined file with all measure indexes with latest two weeks of nyri data
  latest_weeks <- streeteasy_full %>%
    inner_join(nycrentals_updated, by = c("date" = "Week Ending")) %>%
    inner_join(updated_nyc_covid19_hospitalizations, by = "date") %>%
    inner_join(updated_nyc_ui, by = "date") %>%
    inner_join(ot_table_ready, by = "date") %>%
    inner_join(mta_ridership_nyc, by = "date") %>%
    select(date, `Covid-19 Hospitalizations Index`, `Unemployment Claims Index`,
           `Home Sales Index`, `Rental Inventory Index`, `Subway Mobility Index`,
           `Restaurant Reservations Index`) %>%
    filter(date %in% c(week_of_analysis_date, week_of_analysis_date - 7)) %>%
    arrange(date)


  # Creating csv for the week-over-week changes in measure and overall score
  WoW_change <- map_dfr(latest_weeks[2:7], diff) %>%
    mutate(`New York City Recovery Index` = sum(
      map_dbl(
        select(
          filter(
            latest_weeks, date == week_of_analysis_date
          ), -date
        ), ~.x/6
      )) - sum(
        map_dbl(
          select(
            filter(
              latest_weeks, date == week_of_analysis_date - 7
            ), -date
          ), ~.x/6
        )
      )
    ) %>%
    relocate(`New York City Recovery Index`, everything()) %>%
    pivot_longer(everything(), names_to = "DATE", values_to = as.character(week_of_analysis_date))


  index_recovery_overview <- read_csv("./data/nyc_recovery_index_overview.csv",
                                    col_types = "Ddddddd")

  # Adding latest measure scores into overall csv score for stacked density plot
  latest_nyri_scores <- latest_weeks %>%
    filter(date == week_of_analysis_date) %>%
    mutate(across(2:7, ~. / 6))


  index_recovery_overview_latest <- bind_rows(latest_nyri_scores, index_recovery_overview)

},
error = function(e) {
  e_full <- error_cnd(class = "data_update", message = paste("An error occured with the data writing update:",
                                                           e, "on", Sys.Date(), "\n"))

  message(e_full[["message"]])

  return(e_full)
})

if (any(map_lgl(list(ot_update, mta_update, ui_update, covid_update,
                     home_sales_update, rentals_update, data_file_update
), ~class(.x)[2] == "rlang_error"), na.rm = T)) {
  stop("There was an error in the update, check the error log to see more.")
} else {
  message("Data update was successful! Writing files and pushing to Git...")
  write_csv(ot_table_ready, "./data/opentable.csv")
  Sys.sleep(2)
  write_csv(mta_ridership_nyc, "./data/mta_subway.csv")
  Sys.sleep(2)
  write_csv(updated_nyc_ui, "./data/nyc_ui.csv")
  Sys.sleep(2)
  write_csv(updated_nyc_covid19_hospitalizations, "./data/nyc_covid19_hospitalizations.csv")
  Sys.sleep(2)
  write_csv(streeteasy_full, "./data/streeteasy_home_sales.csv")
  Sys.sleep(2)
  write_csv(nycrentals_updated, "./data/streeteasy_rentals.csv")
  Sys.sleep(2)
  write_csv(WoW_change, paste0("./visualizations/", week_of_analysis_date, "_DW_NYC_Recovery_Index_WoW_Changes.csv"))
  Sys.sleep(2)
  write_csv(index_recovery_overview_latest, paste0("./visualizations/", week_of_analysis_date, "_DW_NYC_Recovery_Index_Overview.csv"))
  Sys.sleep(2)
  write_csv(index_recovery_overview_latest, "./data/nyc_recovery_index_overview.csv")
}

