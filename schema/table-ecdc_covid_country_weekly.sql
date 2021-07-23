CREATE TABLE IF NOT EXISTS ecdc_covid_country_weekly(
    iso_a3                            text,
    iso_a2                            text,
    country_name                      text,
    country_name_local                text,
    population                        real,
    date_year                         real,
    date_week                         real,
    ecdc_covid_country_weekly_cases   real,
    ecdc_covid_country_weekly_deaths  real
);
