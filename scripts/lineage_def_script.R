library(tidyverse)
library(DBI)
library(RPostgreSQL)

print(paste(Sys.time(), "started...", sep = " "))


load("/mnt/repo/data/lineage_def.Rdata") #TODO: use env var to point to extra data folder


con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),
                      dbname = Sys.getenv(c("DB")),
                      host = Sys.getenv(c("DB_HOST")),
                      port = Sys.getenv(c("DB_PORT")),
                      user = Sys.getenv(c("SECRET_USERNAME")),
                      password = Sys.getenv(c("SECRET_PASSWORD"))
)
dbSendQuery(con, "TRUNCATE TABLE lineage_def")
print(paste(Sys.time(), "truncated table lineage_def", sep=" ")) 

dbWriteTable(con, "lineage_def", lineage_def , row.names = FALSE, overwrite = TRUE)

dbDisconnect(con)


