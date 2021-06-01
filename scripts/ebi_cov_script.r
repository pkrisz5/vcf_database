# This script checks the `filepath` folder and uploads to the server those coverage data that was not present yet on the server

library(tidyverse)
library(DBI)

print(paste(Sys.time(), "started...", sep = " "))

con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),
  dbname = Sys.getenv(c("DB")),
  host = Sys.getenv(c("DB_HOST")),
  port = Sys.getenv(c("DB_PORT")),
  user = Sys.getenv(c("SECRET_USERNAME")),
  password = Sys.getenv(c("SECRET_PASSWORD"))
)


# Downloads the ID of the already uploaded coverage files

n <- tbl(con, "cov") %>%
  select(ena_run) %>%
  distinct() %>%
  collect()


if (nrow(n) == 0) n <- tibble(ena_run = character())

print(paste(Sys.time(), "number of sample records aready in tabe cov before update", nrow(n), sep = " "))

# Selects the new coverage files and uploads them in bins

filepath <- c("/x_cov/")

ids <- tibble(ena_run = str_remove(list.files(path = filepath, pattern = regex("\\.coverage$")), pattern = ".coverage"))
ids <- ids %>%
  dplyr::filter(!ena_run %in% n$ena_run)
if (nrow(ids) != 0) {
  print(paste(Sys.time(), "number of new files in the folder:", nrow(ids), sep = " "))
  ids <- ids %>%
    mutate(rows = seq.int(nrow(ids))) %>%
    mutate(bin = cut(rows, seq(1, nrow(ids) + 500, 500), right = FALSE)) # this creates bins because if too many files are treated in a single sptep, then it can cause problem, so in a single step data about max 500 samples are uploaded

  for (j in levels(ids$bin)) {
    print(paste(Sys.time(), "processing bin", j, sep = " "))
    cov <- tibble(poz = 1:29903)
    x <- ids %>%
      filter(bin == j)
    x <- as.character(x$ena_run)
    for (i in x) {
      if (file.size(paste(filepath, i, ".coverage", sep = "")) != 0) {
        temp <- read_csv(paste(filepath, i, ".coverage", sep = ""),
          col_names = c("id", "ref", i),
          cols(col_double(), col_character(), col_double())
        )
        if (ncol(temp != 0) & nrow(temp) == 29903) {
          cov <- cbind(cov, temp[3])
        } else {
          print(paste(Sys.time(), "excluded incomplete file:", i, sep = " "))
        }
      } else {
        print(paste(Sys.time(), "excluded empty file:", i, sep = " "))
      }
    }
    if (ncol(cov) != 1) {
      cov <- cov %>%
        pivot_longer(cols = (-1), names_to = "ena_run", values_to = "coverage") %>%
        dplyr::rename(pos = poz) %>%
        dplyr::filter(coverage<100)%>%
        select(ena_run, pos, coverage)

      print(paste(Sys.time(), "appending", nrow(cov), " records in cov", sep = " "))
      dbWriteTable(con, "cov", cov, append = TRUE, row.names = FALSE)
    }
  }
}

n <- tbl(con, "cov") %>%
  select(ena_run) %>%
  distinct() %>%
  collect()

if (nrow(n) == 0) n <- tibble(ena_run = character())

print(paste(Sys.time(), "number of records in table cov", nrow(n), sep = " "))

