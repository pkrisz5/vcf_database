# This script checks the `filepath` folder and uploads to the server those vcf data that was not present yet on the server

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

# Downloads the ID of the already uploaded vcf files

n <- tbl(con, "vcf") %>% 
  select(ena_run) %>%
  distinct()%>%
  collect()

if (nrow(n)==0) n <- tibble(ena_run=character())

print(paste("Number of samples that are aready in the database:", nrow(n), "(time stamp:", Sys.time(), ")", sep=" ")) 

# Selects the new vcf files and uploads them in bins

filepath <- c("/x_vcf/")
ids <- tibble(ena_run=str_remove_all(list.files(path = filepath, pattern = regex("[0-9].annot.vcf")), pattern = ".annot.vcf"))
ids <- ids %>%
  dplyr::filter(!ena_run %in% n$ena_run)

if (nrow(ids)!=0){
  print(paste("Number of new files in the folder:", nrow(ids), "(time stamp:", Sys.time(), ")", sep=" ")) 
  ids <- ids %>%
    mutate(rows=seq.int(nrow(ids))) %>%
    mutate(bin = cut(rows, seq(1, nrow(ids) + 500, 500), right = FALSE)) # this creates bins because if too many files are treated in a single step, then it can cause problem, so in a single step data about max 1000 samples are uploaded
  print(ids)
  ann_name <- c("allele", "annotation", "annotation_impact", "gene_name" ,"gene_id", "feature_type", "feature_id", "transcript_biotype", "rank_", "hgvs_c", "hgvs_p" ,"cdna_pos__cdna_length",  "cds_pos__cds_length" ,"aa_pos__aa_length" , "distance" , "errors_warnings_info") 
  
  for (j in levels(ids$bin)) {
    print(paste("Bin under processing:", j, "(time stamp:", Sys.time(), ")", sep=" ")) 
    vcf <- tibble(`#CHROM` = character(),
                  POS = double(),
                  ID = character(),
                  REF = character(),
                  ALT = character(),
                  QUAL = double(),
                  FILTER = character(),
                  INFO = character())
    
    
    f_list <- ids %>%
      filter(bin==j)
    f_list <- as.character(f_list$ena_run)
    for (f in f_list){
      print(paste("start process:", f, sep= " "))
      if (file.size(paste(filepath, f, ".annot.vcf", sep=""))!=0) {
        vcf_file <- paste(filepath, f, ".annot.vcf", sep="")
        x <- read_tsv(file = vcf_file, skip = 20, col_names=TRUE, cols(`#CHROM` = col_character(),
                                                                       POS = col_double(),
                                                                       ID = col_character(),
                                                                       REF = col_character(),
                                                                       ALT = col_character(),
                                                                       QUAL = col_double(),
                                                                       FILTER = col_character(),
                                                                       INFO = col_character())) %>%
          mutate(ID=f)
        vcf <- rbind(vcf,x)
      } else {
        print(paste("Excluded empty file:", f, sep=" "))
      }
    }
    if (nrow(vcf)!=0){
      vcf <- vcf %>%
        dplyr::rename(chrom = `#CHROM`,
                      pos = POS,
                      ena_run = ID,
                      ref = REF,
                      alt = ALT,
                      qual = QUAL,
                      filter = FILTER,
                      info = INFO)%>%
        separate(col="info", into=c("dp", "af", "sb", "dp4", "ann", "hrun", "indel", "lof", "nmd"), sep = ";", fill="right")
      vcf <- vcf %>%
        mutate(ann = ifelse(vcf$ann=="INDEL", indel, ann)) %>%
        mutate(indel = ifelse(!is.na(vcf$indel), TRUE, FALSE)) 
      k <- max(str_count(vcf$ann, pattern = "\\,")  ) # maximum annotate version
      a <- as_tibble(str_split_fixed(vcf$ann, pattern = "\\,", n=k)  ) 
      vcf <- cbind(vcf, a) %>%
        select(-ann)%>%
        pivot_longer(cols = names(a), values_to = "ann", names_to="ann_num", values_drop_na = TRUE) %>%
        filter(ann!="") %>%
        mutate(ann_num= str_sub(ann_num,start=2))
      vcf$ann_num <- as.integer(vcf$ann_num)
      
      x <- as_tibble(str_split_fixed(vcf$ann, pattern = "\\|", n=16) , column_name = ann_name)
      names(x) <- ann_name
      
      
      vcf <- vcf %>%
        mutate (dp4 = str_remove(dp4, pattern = "DP4=")) %>%
        separate(col = dp4, into = c("count_ref_forward_base", "count_ref_reverse_base", "count_alt_forward_base", "count_alt_reverse_base")) %>%
        bind_cols(x) %>%
        select (-ann) %>%
        select (-allele) %>%
        mutate (lof = ifelse(str_detect(hrun, pattern = "LOF="), hrun, lof))%>% # This fix a problem that sometimees lof and hrun columns are mixed
        mutate (hrun = ifelse(str_detect(hrun, pattern = "LOF="), NA, hrun))%>%
        mutate (dp = str_remove(dp, pattern = "DP=")) %>%
        mutate (af = str_remove(af, pattern = "AF=")) %>%
        mutate (sb = str_remove(sb, pattern = "SB=")) %>%
        mutate (hrun = str_remove(hrun, pattern = "HRUN=")) %>%
        mutate (lof = str_remove(lof, pattern = "LOF=")) %>%
        mutate (nmd = str_remove(nmd, pattern = "NMD=")) %>%
        select (ena_run, everything())
      vcf[vcf==""] <- NA
      vcf$pos <- as.integer(vcf$pos)
      vcf$qual <- as.integer(vcf$qual)
      vcf$dp <- as.integer(vcf$dp)
      vcf$af <- as.numeric(vcf$af)
      vcf$sb <- as.integer(vcf$sb)
      vcf$count_alt_forward_base <- as.integer(vcf$count_alt_forward_base)
      vcf$count_ref_reverse_base <- as.integer(vcf$count_ref_reverse_base)
      vcf$count_ref_forward_base <- as.integer(vcf$count_ref_forward_base)
      vcf$count_ref_reverse_base <- as.integer(vcf$count_ref_reverse_base)
      vcf$hrun <- as.integer(vcf$hrun)
      vcf$distance <- as.integer(vcf$distance)
      dbWriteTable(con, "vcf", vcf , append = TRUE, row.names = FALSE)
    }
  }
}


n <- tbl(con, "vcf") %>% 
  select(ena_run) %>%
  distinct()%>%
  collect()

if (nrow(n)==0) n <- tibble(ena_run=character())

print(paste("Number of samples that are in the database after the process:", nrow(n),  "(time stamp:", Sys.time(), ")", sep=" ")) 
