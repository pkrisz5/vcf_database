# This script checks the `filepath` folder and uploads to the server those vcf data that was not present yet on the server

library(tidyverse)
library(DBI)

print(paste(Sys.time(), "started...", sep = " "))

#FIXME:
manual_excl_id <- c("ERR5471857", "ERR5473614", "ERR5473980", "ERR5474250",    "ERR5479235") # This vcf file is wrong, so needed to exclude manualy

con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),
  dbname = Sys.getenv(c("DB")),
  host = Sys.getenv(c("DB_HOST")),
  port = Sys.getenv(c("DB_PORT")),
  user = Sys.getenv(c("SECRET_USERNAME")),
  password = Sys.getenv(c("SECRET_PASSWORD"))
)

# Downloads the ID of the already uploaded vcf files

n <- tbl(con, "unique_vcf_append") %>%
  select(ena_run) %>%
  collect()
if (nrow(n) == 0) n <- tibble(ena_run = character())

# count how many files (aka ena_run) are uploaded
N <- 0

print(paste(Sys.time(), "number of records in vcf_all table", nrow(n), sep = " "))

# Selects the new vcf files and uploads them in bins

filepath <- c(Sys.getenv(c("DIR_TMP")))
ids <- tibble(ena_run = str_remove_all(list.files(path = filepath, pattern = regex("[0-9].annot.vcf")), pattern = ".annot.vcf"))
ids <- ids %>%
  dplyr::filter(!ena_run %in% n$ena_run) %>% # this removes ena_run ids those are already in the database
  dplyr::filter(!ena_run %in% manual_excl_id) # this removes manually excluded ena_run ids

if (nrow(ids) != 0) {
  print(paste(Sys.time(), "number of new files in the folder:", nrow(ids), sep = " "))
  ids <- ids %>%
    mutate(rows = seq.int(nrow(ids))) %>%
    mutate(bin = cut(rows, seq(1, nrow(ids) + 500, 500), right = FALSE)) # this creates bins because if too many files are treated in a single step, then it can cause problem, so in a single step data about max 1000 samples are uploaded
  ann_name <- c("allele", "annotation", "annotation_impact", "gene_name", "gene_id", "feature_type", "feature_id", "transcript_biotype", "rank_", "hgvs_c", "hgvs_p", "cdna_pos__cdna_length", "cds_pos__cds_length", "aa_pos__aa_length", "distance", "errors_warnings_info")

  for (j in levels(ids$bin)) {
    print(paste(Sys.time(), "processing bin", j, sep = " "))
  vcf <- tibble(chrom = character(),
               pos = double(),
               ena_run = character(),
               ref = character(),
               alt = character(),
               qual = double(),
               filter = character(),
               dp = character(),
               af = character(),
               sb = character(),
               dp4 = character(),
               ann = character(),
               hrun = character(),
               indel = character(),
               lof = character(),
               nmd = character())


    f_list <- ids %>%
      filter(bin == j)
    f_list <- as.character(f_list$ena_run)

    unique_vcf <- tibble(insertion_ts = character(), ena_run = character(), snapshot = character(), integrity = integer())
    ts <- Sys.time() 
    r <- 0
    for (f in f_list) {
	    
      r <- r+1
      unique_vcf[r, 'insertion_ts'] <- ts
      unique_vcf[r, 'ena_run'] <- f
      unique_vcf[r, 'snapshot'] <- filepath

      #print(paste(Sys.time(), "processing file", paste(filepath, f, ".annot.vcf", sep = ""), sep=" "))
      if (file.size(paste(filepath, f, ".annot.vcf", sep = ""))!=0) {
        vcf_file <- paste(filepath, f, ".annot.vcf", sep = "")
            is_nanopore <- str_detect(read_lines(file = vcf_file, skip = 2, n_max = 1), pattern = "bam_to_vcf.py")
            x <- read_tsv(file = vcf_file, comment="##", col_names=TRUE, na = c("", "NA", "."), cols(`#CHROM` = col_character(),
                                                             POS = col_double(),
                                                             ID = col_character(),
                                                             REF = col_character(),
                                                             ALT = col_character(),
                                                             QUAL = col_double(),
                                                             FILTER = col_character(),
                                                             INFO = col_character())) %>%
                    mutate(ID=f)

            if (is_nanopore){
                    x <- x %>%
                        dplyr::rename(chrom = `#CHROM`,
                        pos = POS,
                        ena_run = ID,
                        ref = REF,
                        alt = ALT,
                        qual = QUAL,
                        filter = FILTER,
                        info = INFO)%>%
                        separate(col="info", into=c("dp", "af", "dp4", "ann", "indel", "lof", "nmd"), sep = ";", fill="right") %>%
                        add_column(sb=NA, .after = "af")%>%
                        add_column(hrun=NA, .before = "indel")

              
            } else {

                    x <- x %>%
                        dplyr::rename(chrom = `#CHROM`,
                        pos = POS,
                        ena_run = ID,
                        ref = REF,
                        alt = ALT,
                        qual = QUAL,
                        filter = FILTER,
                        info = INFO)%>%
                        separate(col="info", into=c("dp", "af", "sb", "dp4", "ann", "hrun", "indel", "lof", "nmd"), sep = ";", fill="right")
            }
        vcf <- rbind(vcf,x)
          unique_vcf[r, 'integrity'] <- 0
      } else {
        print(paste(Sys.time(), "excluded empty file", f, sep = " "))
          unique_vcf[r, 'integrity'] <- 1
      }
    }

    if (nrow(vcf) != 0) {
      vcf <- vcf %>%
      mutate(ann = ifelse(vcf$ann=="INDEL", indel, ann)) %>%
      mutate(indel = ifelse(!is.na(vcf$indel), TRUE, FALSE)) 
    k <- max(str_count(vcf$ann, pattern = "\\,"), 1L  ) # maximum annotate version
    a <- as_tibble(str_split_fixed(vcf$ann, pattern = "\\,", n=k)  ) 
    vcf <- cbind(vcf, a) %>%
      select(-ann)%>%
      pivot_longer(cols = names(a), values_to = "ann", names_to="ann_num", values_drop_na = TRUE) %>%
      filter(ann!="") %>%
      mutate(ann_num= str_sub(ann_num,start=2))
    vcf$ann_num <- as.integer(vcf$ann_num)
    
    # x <- as_tibble(str_split_fixed(vcf$ann, pattern = "\\|", n=16) , column_name = ann_name)
    x <- data.frame(str_split_fixed(vcf$ann, pattern = "\\|", n=16))
    #x <- as_tibble(str_split_fixed(vcf$ann, pattern = "\\|", n=16), .name_repair = 'unique')
    names(x) <- ann_name

      vcf <- vcf %>%
        mutate(dp4 = str_remove(dp4, pattern = "DP4=")) %>%
        separate(col = dp4, into = c("count_ref_forward_base", "count_ref_reverse_base", "count_alt_forward_base", "count_alt_reverse_base")) %>%
        bind_cols(x) %>%
        select(-ann) %>%
        select(-allele) %>%
        mutate(lof = ifelse(str_detect(hrun, pattern = "LOF="), hrun, lof)) %>% # This fix a problem that sometimees lof and hrun columns are mixed
        mutate(hrun = ifelse(str_detect(hrun, pattern = "LOF="), NA, hrun)) %>%
        mutate(dp = str_remove(dp, pattern = "DP=")) %>%
        mutate(af = str_remove(af, pattern = "AF=")) %>%
        mutate(sb = str_remove(sb, pattern = "SB=")) %>%
        mutate(hrun = str_remove(hrun, pattern = "HRUN=")) %>%
        mutate(lof = str_remove(lof, pattern = "LOF=")) %>%
        mutate(nmd = str_remove(nmd, pattern = "NMD=")) %>%
        select(ena_run, everything()) %>%
        dplyr::filter(annotation != "downstream_gene_variant") %>%
        dplyr::filter(annotation != "upstream_gene_variant")
      vcf[vcf == ""] <- NA
      vcf$pos <- as.integer(vcf$pos)
      vcf$qual <- as.integer(vcf$qual)
      vcf$dp <- as.integer(vcf$dp)
      vcf$af <- as.numeric(vcf$af)
      #vcf <- dplyr::filter(vcf, af>=0.1)
      vcf$sb <- as.integer(vcf$sb)
      vcf$count_alt_forward_base <- as.integer(vcf$count_alt_forward_base)
      vcf$count_ref_reverse_base <- as.integer(vcf$count_ref_reverse_base)
      vcf$count_ref_forward_base <- as.integer(vcf$count_ref_forward_base)
      vcf$count_ref_reverse_base <- as.integer(vcf$count_ref_reverse_base)
      vcf$hrun <- as.integer(vcf$hrun)
      vcf$distance <- as.integer(vcf$distance)
      dbWriteTable(con, name = "vcf_all_append", value = vcf, append = TRUE, row.names = FALSE)
      dbWriteTable(con, "unique_vcf_append", unique_vcf, append = TRUE, row.names = FALSE)
      N <- N + r

      # Remove those tmp files that are successfully appended to table
      for (f in f_list) {
       file.remove(paste(filepath, f, ".annot.vcf", sep = ""))
      }
      print(paste(Sys.time(), "files removed, loop next", sep = " "))
    }
  }
}


print(paste(Sys.time(), "number of records appended to vcf_all_appende", N, sep = " "))

