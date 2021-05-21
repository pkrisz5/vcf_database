# This script checks the `filepath` folder and uploads to the server those coverage data that was not present yet on the server

library(tidyverse)
library(DBI)

print(paste("Update started:",  Sys.time(), sep=" ")) 

con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),
                      dbname = Sys.getenv(c("DB")),
                      host = Sys.getenv(c("DB_HOST")),
                      port = Sys.getenv(c("DB_PORT")),
                      user = Sys.getenv(c("SECRET_USERNAME")),
                      password = Sys.getenv(c("SECRET_PASSWORD"))
)


# Downloads the ID of the already uploaded coverage files

n <- tbl(con, "cov") %>% 
  filter(pos==1L) %>%
  select(ena_run) %>%
  collect()

if (nrow(n)==0) n <- tibble(ena_run=character())

print(paste("Number of samples that are aready in the database before update:", nrow(n), "(time stamp:", Sys.time(), ")", sep=" ")) 

# Selects the new coverage files and uploads them in bins


filepath <- c("/x_cov/")

ids <- tibble(ena_run=str_remove(list.files(path = filepath, pattern = regex("\.coverage$")), pattern = ".coverage"))
ids <- ids %>%
  dplyr::filter(!ena_run %in% n$ena_run) 
if (nrow(ids)!=0){
  print(paste("Number of new files in the folder:", nrow(ids), "(time stamp:", Sys.time(), ")", sep=" ")) 
  ids <- ids %>%
    mutate(rows=seq.int(nrow(ids))) %>%
    mutate(bin = cut(rows, seq(1, nrow(ids) + 500, 500), right = FALSE)) # this creates bins because if too many files are treated in a single sptep, then it can cause problem, so in a single step data about max 1000 samples are uploaded
  
  for (j in levels(ids$bin)) {
    print(paste("Bin under processing:", j, "(time stamp:", Sys.time(), ")", sep=" ")) 
    cov <- tibble(poz=1:29903)
    x <- ids %>%
      filter(bin==j)
    x <- as.character(x$ena_run)
    for (i in x){
      if (file.size(paste(filepath , i, ".coverage", sep = ""))!=0) {
        temp <- read_csv(paste(filepath , i, ".coverage", sep = ""), 
                         col_names = c("id", "ref", i), 
                         cols( col_double(), col_character(), col_double()))
        if (ncol(temp!=0) & nrow(temp)==29903){
          cov <- cbind(cov, temp[3])
        } else {
          print(paste("Excluded non-complete file:", i, sep=" "))
        }
        
        
      } else {
        print(paste("Excluded empty file:", i, sep=" "))
      }
    }
    if (ncol(cov)!=1){
      cov <- cov %>%
        pivot_longer(cols = (-1), names_to = "ena_run", values_to = "coverage")%>%
        dplyr::rename(pos=poz)%>%
        select(ena_run,pos, coverage )
      
      print (ncol(cov))
      ###dbWriteTable(con, "cov", cov , append = TRUE, row.names = FALSE)
    }
  }
}


n <- tbl(con, "cov") %>% 
  filter(pos==1L) %>%
  select(ena_run) %>%
  collect()

if (nrow(n)==0) n <- tibble(ena_run=character())

print(paste("Number of samples that are aready in the database after the process:", nrow(n), "(time stamp:", Sys.time(), ")", sep=" ")) 
