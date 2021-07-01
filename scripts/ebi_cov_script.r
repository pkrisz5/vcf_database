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

n <- tbl(con, "unique_cov_append") %>%
  select(ena_run) %>%
  collect()
if (nrow(n) == 0) n <- tibble(ena_run = character())

# count how many files (aka ena_run) are uploaded
N <- 0

print(paste(Sys.time(), "number of sample records aready in tabe cov before update", nrow(n), sep = " "))

# Selects the new coverage files and uploads them in bins

filepath <- c(Sys.getenv(c("DIR_TMP")))

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

    unique_cov <- tibble(insertion_ts = as.POSIXct(NA), ena_run = character(), snapshot = character(), integrity = integer())
    ts <- Sys.time() 
    r <- 0

    for (i in x) {
	    
      r <- r+1
      unique_cov[r, 'insertion_ts'] <- ts
      unique_cov[r, 'ena_run'] <- i
      unique_cov[r, 'snapshot'] <- filepath

      if (file.size(paste(filepath, i, ".coverage", sep = "")) != 0) {
        temp <- read_csv(paste(filepath, i, ".coverage", sep = ""),
          col_names = c("id", "ref", i),
          cols(col_double(), col_character(), col_double())
        )
        if (ncol(temp != 0) & nrow(temp) == 29903) {
          cov <- cbind(cov, temp[3])
          unique_cov[r, 'integrity'] <- 0
        } else {
          print(paste(Sys.time(), "excluded incomplete file:", i, sep = " "))
          unique_cov[r, 'integrity'] <- 2
        }
      } else {
        print(paste(Sys.time(), "excluded empty file:", i, sep = " "))
          unique_cov[r, 'integrity'] <- 1
      }
    }
    if (ncol(cov) != 1) {
      cov <- cov %>%
        pivot_longer(cols = (-1), names_to = "ena_run", values_to = "coverage") %>%
        dplyr::rename(pos = poz) %>%
        dplyr::filter(coverage<100)%>%
        select(ena_run, pos, coverage)

      print(paste(Sys.time(), "appending", nrow(cov), " records in cov", sep = " "))
      dbWriteTable(con, "cov_append", cov, append = TRUE, row.names = FALSE)
      dbWriteTable(con, "unique_cov_append", unique_cov, append = TRUE, row.names = FALSE)
      N <- N + r

      # Remove those tmp files that are successfully appended to table
      for (f in x) {
       file.remove(paste(filepath, f, ".coverage", sep = ""))
      }
      print(paste(Sys.time(), "files removed, loop next", sep = " "))
    }
  }
}


print(paste(Sys.time(), "number of records appended to cov_append", N, sep = " "))

