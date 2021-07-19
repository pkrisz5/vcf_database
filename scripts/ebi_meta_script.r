library(tidyverse)
library(DBI)
library(lubridate)
library(RCurl)
library(stringr)

print(paste(Sys.time(), "started...", sep=" ")) 

												

url <- getURL(url = "https://www.ebi.ac.uk/ena/portal/api/search?result=read_run&query=tax_tree(2697049)&fields=accession%2Csample_accession%2Cexperiment_accession%2Cstudy_accession%2Cdescription%2Ccountry%2Ccollection_date%2Cfirst_created%2Cfirst_public%2Chost%2Chost_sex%2Chost_tax_id%2Chost_body_site%2Cbio_material%2Cculture_collection%2Cinstrument_model%2Cinstrument_platform%2Clibrary_layout%2Clibrary_name%2Clibrary_selection%2Clibrary_source%2Clibrary_strategy%2Csequencing_method%2Cisolate%2Cstrain%2Cbase_count%2Ccollected_by%2Cbroker_name%2Ccenter_name%2Csample_capture_status%2Cfastq_ftp%2Ccollection_date_submitted%2Cchecklist&format=tsv&limit=0", 
              httpheader =c( 'Content-Type' = "application/x-www-form-urlencoded")                )
d1 <- read_tsv(file = url , col_types = cols(.default = "c"))

print(paste(Sys.time(), "number of rows downloaded:", nrow(d1), sep=" ")) 

if (nrow(d1) < 1) {
 q(status = 2)
}

# There some run_accession id was not unique, this part below fix this

double_id <- d1%>%
  group_by(run_accession) %>%
  summarise(n=n()) %>%
  filter(n>1)

d <- d1 %>%
  filter(!(run_accession%in%as.character(double_id$run_accession)))
dd <- d1 %>%
  filter(run_accession%in%as.character(double_id$run_accession)) %>%
  filter(!is.na(checklist))
d <- rbind(d, dd)

x <- d %>%
  mutate( yn = str_detect(country, pattern = ":")) %>%
  mutate(yn = ifelse(is.na(yn), "FALSE", yn))

xx1 <- filter(x, yn==TRUE) %>%
  mutate (clean_country = country)%>%
  separate(col = clean_country, into = c("clean_country", "del"), sep = ":") %>%
  select(- del) %>%
  select(- yn)

xx2 <- filter(x, yn!=TRUE) %>%
  mutate (clean_country = country)%>%
  select(- yn)

clean_meta <- rbind(xx1, xx2) %>%
  mutate(clean_host = host) %>%
  mutate(clean_host = ifelse(clean_host%in%c("homan", "homo sapiens", "Homo sapiens", "Homo Sapiens", "homosapiens", "Human", "sapiens") , "Homo sapiens", clean_host)) %>%
  select(run_accession, collection_date, clean_country, clean_host, everything()) %>%
  dplyr::rename(ena_run="run_accession") 

clean_meta <- clean_meta %>%
  mutate(clean_collection_date = ifelse(str_count(collection_date_submitted)==10, collection_date, NA))

clean_meta <- clean_meta %>%
  mutate(date_isoweek=isoweek(clean_collection_date)) %>%
  mutate(date_isoyear=isoyear(clean_collection_date))

clean_meta$collection_date <- as_date(clean_meta$collection_date)
clean_meta$first_created <- as_date(clean_meta$first_created)
clean_meta$first_public <- as_date(clean_meta$first_public)
clean_meta$host_tax_id <- as.numeric(clean_meta$host_tax_id)
clean_meta$base_count <- as.numeric(clean_meta$base_count)
clean_meta$clean_collection_date <- as_date(clean_meta$clean_collection_date)


print(paste(Sys.time(), "parsed", sep=" ")) 

con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),
                      dbname = Sys.getenv(c("DB")),
                      host = Sys.getenv(c("DB_HOST")),
                      port = Sys.getenv(c("DB_PORT")),
                      user = Sys.getenv(c("SECRET_USERNAME")),
                      password = Sys.getenv(c("SECRET_PASSWORD"))
)
dbSendQuery(con, "TRUNCATE TABLE meta_append")
print(paste(Sys.time(), "truncated table meta_append", sep=" ")) 

dbWriteTable(con, "meta_append", clean_meta , append = TRUE, row.names = FALSE)

n <- tbl(con, "meta_append") %>% 
  count()%>%
  collect

print(paste(Sys.time(), "wrote", n$n, "records in table meta_append", sep=" ")) 

if (n$n == 0) {
  q(status = 1)
}
