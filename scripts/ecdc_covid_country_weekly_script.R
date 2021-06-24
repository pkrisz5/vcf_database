library(tidyverse)
library(DBI)
library(RPostgreSQL)

print(paste(Sys.time(), "started...", sep = " "))

data <- read_csv("https://opendata.ecdc.europa.eu/covid19/nationalcasedeath/csv")
load("../data/country_iso.Rdata") #TODO: use env var to point to extra data folder

data <- data %>%
     select(country_code, year_week, indicator, weekly_count) %>%
     drop_na(country_code)%>%
     pivot_wider(values_from = weekly_count, names_from = indicator)%>%
     separate(year_week, into = c("date_year", "date_week"), sep="-")%>%
     rename(iso_a3 = country_code,
            ecdc_covid_country_weekly_cases = cases,
            ecdc_covid_country_weekly_deaths = deaths)

data <- data%>%
     mutate(date_year = as.numeric(date_year),
            date_week = as.numeric(date_week),
            ecdc_covid_country_weekly_cases = as.numeric(ecdc_covid_country_weekly_cases),
            ecdc_covid_country_weekly_deaths = as.numeric(ecdc_covid_country_weekly_deaths))

fin <- country_iso %>%
     left_join(data)


con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),
                      dbname = Sys.getenv(c("DB")),
                      host = Sys.getenv(c("DB_HOST")),
                      port = Sys.getenv(c("DB_PORT")),
                      user = Sys.getenv(c("SECRET_USERNAME")),
                      password = Sys.getenv(c("SECRET_PASSWORD"))
)

dbWriteTable(con, "ecdc_covid_country_weekly", fin , row.names = FALSE, overwrite = TRUE)

dbDisconnect(con)


