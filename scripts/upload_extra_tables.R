# Uploading variois tables from different sources

library(tidyverse)
library(DBI)

con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),
                      dbname = Sys.getenv(c("DB")),
                      host = Sys.getenv(c("DB_HOST")),
                      port = Sys.getenv(c("DB_PORT")),
                      user = Sys.getenv(c("SECRET_USERNAME")),
                      password = Sys.getenv(c("SECRET_PASSWORD"))
)



##################################################################
# Table: "table_description"

table_description <- read_tsv("https://raw.githubusercontent.com/pkrisz5/vcf_database/main/data/table_description.tsv",
                              col_types = "cccc")
dbWriteTable(con, "table_description", table_description , overwrite = TRUE, row.names = FALSE)


##################################################################
# Table: "column_description"

column_description <- read_tsv("https://raw.githubusercontent.com/pkrisz5/vcf_database/main/data/column_description.tsv",
                               col_types = "ccc")
dbWriteTable(con, "column_description", column_description , overwrite = TRUE, row.names = FALSE)


##################################################################
# Table: 'primer_artic_v3'

url <- c("https://raw.githubusercontent.com/joshquick/artic-ncov2019/master/primer_schemes/nCoV-2019/V3/nCoV-2019.tsv")
primer_artic_v3_seq <- read_tsv(file = url,
                                skip = 1,
                                col_names = c("name",	"pool", "sequence", "length",	"gc_pct", "tm"),
                                col_types = c("cccccc")) %>%
  select(name, sequence)
url <- c("https://raw.githubusercontent.com/joshquick/artic-ncov2019/master/primer_schemes/nCoV-2019/V3/nCoV-2019.primer.bed")
primer_artic_v3 <- read_tsv(file = url,
                            col_names = c("chrom", "start", "end", "name", "primerpool", "strand" ),
                            col_types = c("ciicic")) %>%
  dplyr::mutate(start = start+1) %>%
  right_join(primer_artic_v3_seq, by="name")
dbWriteTable(con, "primer_artic_v3", primer_artic_v3 , overwrite = TRUE, row.names = FALSE)


##################################################################
# Table: 'primer_artic_v4'

url <- c("https://raw.githubusercontent.com/joshquick/artic-ncov2019/master/primer_schemes/nCoV-2019/V4/SARS-CoV-2.primer.bed")
primer_artic_v4 <- read_tsv(file = url,
                            col_names = c("chrom", "start", "end", "name", "primerpool", "strand", "sequence" ),
                            col_types = c("ciicicc")) %>%
  dplyr::mutate(start = start+1)
dbWriteTable(con, "primer_artic_v4", primer_artic_v4 , overwrite = TRUE, row.names = FALSE)


##################################################################
# Table: "pcr_primers"

url <- c("https://raw.githubusercontent.com/pkrisz5/vcf_database/main/data/pcr_primers.tsv")
pcr_primers <- read_tsv(file = url, skip = 1,
                        col_names = c("target_gene", "origin", "country", "type", "primer_name", "primer_set", "original_primer_name",
                                      "target_sequence", "target_sequence_start_post", "target_sequence_end_pos", "primer_size_bp", "reference_genome",
                                      "update_time", "doi", "reference", "other_reference" ),
                        col_types = c("cccccccciiiccccc")) 

dbWriteTable(con, "pcr_primers", pcr_primers , overwrite = TRUE, row.names = FALSE)
dbSendQuery(con, "GRANT SELECT ON pcr_primers TO kooplex_reader;")


##################################################################
# Table: "amino_acid_symbol"

amino_acid_symbol <- read_tsv("https://raw.githubusercontent.com/pkrisz5/vcf_database/main/data/amino_acid_symbol.tsv",
                              col_types = "ccc")
dbWriteTable(con, "amino_acid_symbol", amino_acid_symbol , overwrite = TRUE, row.names = FALSE)

##################################################################
# Table: "lamp_primers"

url <- c("https://raw.githubusercontent.com/pkrisz5/vcf_database/main/data/lamp_primers.tsv")
lamp_primers <- read_tsv(file = url, skip = 1,
                        col_names = c("target_gene",	"origin",	"country",	"cat_type",	"primer_name",
                                      "type",	"original_primer_name",	"primer_sequence_5_3",	"target_sequence_start_pos",
                                      "target_sequence_end_pos",	"primer_size_bp",	"reference_genome",
                                      "update_time",	"doi",	"reference" ),
                        col_types = c("cccccccciiicccc")) 

dbWriteTable(con, "lamp_primers", lamp_primers , overwrite = TRUE, row.names = FALSE)
dbSendQuery(con, "GRANT SELECT ON lamp_primers TO kooplex_reader;")

