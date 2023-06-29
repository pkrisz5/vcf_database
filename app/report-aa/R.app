library(shiny)
library(DBI)
library(shinydashboard)
library(tidyverse)
library(lubridate)
library(plotly)
library(highcharter)
library(pool)
library(config)
library(shinyBS)
library(ISOweek)
library(NGLVieweR)
library(RColorBrewer)
library(shinyThings)
library(jsTreeR) 
library(shinyWidgets)
library(shinybusy)
library(glue)
library(jsonlite)
# The custom variant part needs to be rewritten!!!! The data is false now!
app_version <- "v_browser_005.003"

# Connection details

# config <- config::get()
# con <- dbPool(
#      drv = RPostgreSQL::PostgreSQL(),
#      dbname = config$dbname,
#      host = config$host,
#      port = config$port,
#      user = config$user,
#      password = config$password,
#      option = config$option
# )
# 
# onStop(function() {
#      poolClose(con)
# })

url <- "http://157.181.172.113:8080"


d.load_custom <- function (url, endpoint, param = "", schema = "&schema_key=public"){
     jsonlite::fromJSON(txt = paste(url, "/", endpoint, "/", param, schema, sep=""))
}

d.load <- function (url, endpoint, param = "", schema = "&schema_key=public"){
     jsonlite::fromJSON(txt = paste(url, "/", endpoint, "/", param, schema, sep=""))[-1]
}

# x <- d.load_custom(url = url, endpoint = "filter_custom_browser_cov", param = "?limit=5&included=p.Asp80Ala,p.Asp215Gly&excluded=p.Asp77Al,p.Asp102Ala")
# x <- d.load(url = url, endpoint = "filter_custom_browser_cov_time", param = "?limit=5&included=p.Asp80Ala,p.Asp215Gly&excluded=p.Asp77Al,p.Asp102Ala")

# Configuration

colorstw <- c(brewer.pal(n=8, name="Set2"),brewer.pal(n=12, name="Paired")[-c(3,7)], "#575858", "#c7f89c")[1:19]

# Tables from database

app_country_samples <- d.load(url = url, endpoint = "country_samples", param = "?limit=100000000")
app_lineage <- d.load(url = url, endpoint = "lineage", param = "?limit=100000000")
app_human_meta_mv <- d.load(url = url, endpoint = "human_meta_mv", param = "?limit=100000000")
app_human_meta_mv$date <- date(app_human_meta_mv$date)
app_human_meta_mv_jhd <- d.load(url = url, endpoint = "human_meta_mv_jhd", param = "?limit=100000000")
app_human_meta_mv_jhd$date <- date(app_human_meta_mv_jhd$date)
lineage_def_data <- d.load(url = url, endpoint = "lineage_def", param = "?limit=100000000")
app_lineage$collection_date <- date(app_lineage$collection_date)
app_new_cases <- d.load(url = url, endpoint = "new_cases_jhd", param = "?limit=100000000")
app_new_cases$date <- date(app_new_cases$date)
app_variants_weekly <- d.load(url = url, endpoint = "variants_weekly", param = "?limit=100000000")
unique_ena_run_summary <- d.load(url = url, endpoint = "unique_ena_run_summary", param = "?limit=100000000")



# 
# app_country_samples1 <- tbl(con, "app_country_samples_full") %>%
#      collect()%>%
#      as.data.frame()

# app_lineage1 <- tbl(con, "app_lineage") %>%
#      collect()

# app_human_meta_mv1 <- tbl(con, "app_human_meta_mv") %>%
#      collect()

# app_human_meta_mv_jhd1 <- tbl(con, "app_human_meta_mv_jhd") %>%
#      collect()

# lineage_def_data1 <- tbl(con, "lineage_def") %>%
#      collect()

# app_new_cases1 <- tbl(con, "app_new_cases_jhd") %>%
# collect()

# app_variants_weekly1 <- tbl(con, "app_variants_weekly") %>%
#      collect()



# unique_ena_run_summary1 <- tbl(con, "unique_ena_run_summary") %>%
#      collect()

variants_weekly <- app_variants_weekly %>%
     pivot_wider(names_from = variant_id, values_from = weekly_variant_sample)

variants_weekly$cases_with_variant_id <- rowSums(variants_weekly[, c(-1, -2, -3)], na.rm = TRUE)

variant_master_table <- app_new_cases %>%
     left_join(variants_weekly) %>%
     mutate(`Non-sequenced new cases` = cases - weekly_sample) %>%
     dplyr::select(-weekly_sample, -cases, -cases_with_variant_id) %>%
     pivot_longer(cols = !any_of(c("country", "date", "date_year", "date_week")))

variants <- unique(app_lineage$variant_id)
variants <- variants[variants!="Not analysed yet " & variants!="Other variant" ]

# Tree
makeNodes <- function(leaves){
     dfs <- lapply(strsplit(leaves, "/"), function(s){
          item <-
               Reduce(function(a,b) paste0(a,"/",b), s[-1], s[1], accumulate = TRUE)
          data.frame(
               item = item,
               parent = c("root", item[-length(item)]),
               stringsAsFactors = FALSE
          )
     })
     dat <- dfs[[1]]
     for(i in 2:length(dfs)){
          dat <- base::merge(dat, dfs[[i]], all=TRUE)
     }
     f <- function(parent){
          i <- match(parent, dat$item)
          item <- dat$item[i]
          children <- dat$item[dat$parent==item]
          label <- tail(strsplit(item, "/")[[1]], 1)
          if(length(children)){
               list(
                    text = label,
                    data = list(value = item),
                    children = lapply(children, f)
               )
          }else{
               list(text = label, data = list(value = item))
          }
     }
     lapply(dat$item[dat$parent == "root"], f)
}


# collect_selected_variant <- function(con, variants, exclusion=c("")) {
#      
#      n_variants <- length(variants)
#      sql_query1 <-glue_sql("
#   DROP TABLE IF EXISTS temp1; 
#    -- DROP TABLE IF EXISTS temp2; 
#    
# 
#    WITH x1 AS (
#      SELECT DISTINCT runid, hgvs_p, country_name, ena_run, collection_date
#       FROM aa_mutation_
#           WHERE (NOT hgvs_p IN ({exclusion*}))
#            AND (hgvs_p IN ( {variants*}))
#    )
#   
#     SELECT max(country_name) as country, ena_run, count(country_name), collection_date
#        INTO TEMP temp1
#         FROM x1
#           GROUP BY ena_run, collection_date
#            HAVING count(ena_run) = {n_variants};
#        
#     SELECT country, count(*)
#       FROM  temp1
#       GROUP BY country; 
#     ", .con = con)
#      
#      sql_query2 <-glue_sql("
#     WITH temp2 AS (
#         SELECT *
# 	        FROM test_background_sample_counts tbsc 
# 	        WHERE collection_date > date ('2019-04-24') 
#     ),
#     temp3 AS (
#     SELECT temp1.collection_date as collection_date, temp1.country as country, count(*)
#       FROM  temp1
#       GROUP BY country, collection_date
#     )
#     
#     SELECT temp2.collection_date, temp2.country, temp2.count-coalesce(temp3.count, 0) AS other_count, coalesce(temp3.count, 0) AS variant_count 
#       FROM temp2
#       LEFT JOIN temp3 USING (collection_date, country) 
#     
#     
#     ; 
#     ", .con = con)
#      
#      d1 <- dbGetQuery(con, sql_query1)
#      d2 <- dbGetQuery(con, sql_query2)
#      return(list(table1=d1,table2=d2))
# }
# 


# collect_selected_variant_with_cov <- function(con, variants, exclusion=c("")) {
#      # variants <- c("D80A"="p.Asp80Ala",
#      #               "D215G"="p.Asp215Gly",
#      #               #"LAL242-244del"="p.Ala243_Leu244del", #seems like these variants are not called correctly
#      #               #"R246I"="p.Arg246Ile", #seems like these variants are not called correctly
#      #               "K417N"="p.Lys417Asn",
#      #               "E484K"="p.Glu484Lys",
#      #               "N501Y"="p.Asn501Tyr",
#      #               "D614G"="p.Asp614Gly",
#      #               "A701V"="p.Ala701Val")
#      n_variants <- length(variants)
#      variants_exclusion <- str_c(variants, ", ", exclusion, collapse = TRUE)
#      aa_pos <- as.integer(str_sub(strsplit(str_remove_all(variants_exclusion, pattern = " "), ",")[[1]],  start = 6, end = -4))
#      sql_query1 <-glue_sql("
#   DROP TABLE IF EXISTS temp1; 
#   
#   WITH small_cov_s_pos AS (
#      SELECT *
#      FROM cov_s_pos
#      --LIMIT 10000000000
#   ),
#   neg_select AS (
#         SELECT runid
#           FROM small_cov_s_pos
#           WHERE pos_aa IN ({aa_pos*})
#           GROUP BY runid
#   ),
#   aa AS (
#       SELECT DISTINCT runid, hgvs_p, country_name, ena_run, collection_date
#       FROM aa_mutation_
#           WHERE (NOT hgvs_p IN ({exclusion*}))
#            AND (hgvs_p IN ( {variants*}))
#      --LIMIT 100000
#   
#   ),
#   
#   aa2 AS (
#     SELECT *
#      FROM aa
#       LEFT JOIN neg_select ON aa.runid = neg_select.runid
#       WHERE neg_select.runid IS NULL
#           -- LIMIT 100000
#   )
#   
#   SELECT max(country_name) as country, ena_run, count(country_name), collection_date
#   INTO TEMP temp1
#    FROM aa2
#      GROUP BY ena_run, collection_date
#      HAVING count(ena_run) = {n_variants}
#   --limit 100
# ;
# 
#     SELECT country, count(*)
#       FROM  temp1
#       GROUP BY country; 
# 
#         ", .con = con)
#      
#      sql_query2 <-glue_sql("
#     WITH temp2 AS (
#         SELECT *
# 	        FROM test_background_sample_counts tbsc 
# 	        WHERE collection_date > date ('2019-04-24') 
#     ),
#     temp3 AS (
#     SELECT temp1.collection_date as collection_date, temp1.country as country, count(*)
#       FROM  temp1
#       GROUP BY country, collection_date
#     )
#     
#     SELECT temp2.collection_date, temp2.country, temp2.count-coalesce(temp3.count, 0) AS other_count, coalesce(temp3.count, 0) AS variant_count 
#       FROM temp2
#       LEFT JOIN temp3 USING (collection_date, country) 
#     
#     
#     ; 
#     ", .con = con)
#      
#      d1 <- dbGetQuery(con, sql_query1)
#      d2 <- dbGetQuery(con, sql_query2)
#      return(list(table1=d1,table2=d2))
# }
# 
# 


convert_aa_1_to_3 <- function(aa_1){
     str_replace_all(aa_1, c("^D$" = "Asp",
                             "^A$" = "Ala",
                             "^R$" = "Arg",
                             "^N$" = "Asn",
                             "^C$" = "Cys",
                             "^E$" = "Glu",
                             "^Q$" = "Gln",
                             "^G$" = "Gly",
                             "^H$" = "His",
                             "^I$" = "Ile",
                             "^L$" = "Leu",
                             "^K$" = "Lys",
                             "^M$" = "Met",
                             "^F$" = "Phe",
                             "^P$" = "Pro",
                             "^S$" = "Ser",
                             "^T$" = "Thr",
                             "^W$" = "Trp",
                             "^Y$" = "Tyr",
                             "^V$" = "Val"))
}     

convert_mut_1_to_3 <- function(mut_list) {
     x <- strsplit(str_remove_all(mut_list, pattern = " "), ",")[[1]]
     x1 <- convert_aa_1_to_3(str_sub(x, start = 1, end = 1))
     x2 <- str_sub(x, start = 2, end = -2)
     x3 <- convert_aa_1_to_3(str_sub(x, start = -1, end = -1))
     str_c("p.",x1, x2, x3, sep = "")
}
############################################################################################
# User interface of the app
############################################################################################

ui <- dashboardPage(
     
     dashboardHeader(title = "CoVEO"),
     
     dashboardSidebar(
          sidebarMenu(
               id = "sidebar",
               menuItem("Samples from countries",
                        icon = icon("map-marked-alt"), startExpanded = TRUE,
                        menuSubItem("Graphs", tabName = "country_graph"),
                        menuSubItem("Maps", tabName = "country_map")
               ),
               menuItem("Variants", tabName = "variants", icon = icon("chart-bar")),
               menuItem("Variants (VOC/VUI selection)", tabName = "lineage_graph_lineage", icon = icon("chart-bar")),
               menuItem("Variants (Country selection)", tabName = "lineage_graph_country", icon = icon("chart-bar")),
               menuItem("Custom variant browser", tabName = "custom_variant", icon = icon("chart-bar")),
               menuItem("Info", tabName = "menu_info", icon = icon("info"))
          )
     ),
     
     dashboardBody(
          add_busy_spinner(spin = "fading-circle", position = "top-right", timeout = 1000),
          tags$head(tags$style(HTML('
      .content-wrapper {
        background-color: #ffffff;
      }
    '
          ))),
          tabItems(
               tabItem(
                    ## Samples from countries //Graphs         
                    tabName = "country_graph",
                    tabsetPanel(
                         type = "tabs",
                         tabPanel(
                              "EU+UK",
                              column( width = 4,
                                      radioButtons("eu_graph_type",
                                                   label = "Visualized data",
                                                   choices = c("Weekly sample number", "Cumulative sample number", "Percent of sequenced samples"),
                                                   inline = FALSE, selected = "Weekly sample number"
                                      ),
                                      
                              ),
                              column( width = 4,
                                      radioSwitchButtons("eu_log_lin",
                                                         label = "Y-axis",
                                                         choices = c("linear", "logarithmic"),
                                                         selected = "linear",
                                                         selected_background = "#367fa9"
                                      )
                              ),
                              box(
                                   title = "",
                                   status = "primary",
                                   height = "650", width = "12", solidHeader = FALSE,
                                   column(
                                        width = 12,
                                        highchartOutput("euPlot", height = "600px"),
                                   )
                              )
                         ),
                         
                         tabPanel(
                              "World",
                              column( width = 4,
                                      radioButtons("world_graph_type",
                                                   label = "Visualized data",
                                                   choices = c("Weekly sample number", "Cumulative sample number", "Percent of sequenced samples"),
                                                   inline = FALSE, selected = "Weekly sample number"
                                      ),
                              ),
                              column( width = 4,
                                      radioSwitchButtons("world_log_lin",
                                                         label = "Y-axis",
                                                         choices = c("linear", "logarithmic"),
                                                         selected = "linear",
                                                         selected_background = "#367fa9"
                                      )
                              ),
                              column( width = 4,
                                      selectInput("world_select_country",
                                                  label = span("Select countries", bsButton("q_select_country_world", label = "", icon = icon("info-circle"), style = "secondary", size = "small")),
                                                  choices = unique(app_human_meta_mv$country_name),
                                                  selected = c("Spain", "Australia", "Brazil"),
                                                  multiple = TRUE
                                      )
                              ),
                              bsPopover(
                                   id = "q_select_country_world", title = "Select countries",
                                   content = paste0('Click to the empty region of the box below and a dropdown menu appears where you can add more countries. Select the unwanted country and use "del" button on you keyboard to clear it from the list'),
                                   trigger = "hover",
                                   options = list(container = "body")
                              ),
                              box(
                                   title = "",
                                   status = "primary",
                                   height = "650", width = "12", solidHeader = FALSE,
                                   column(
                                        width = 12,
                                        highchartOutput("worldPlot", height = "600px"),
                                   )
                              )
                         )
                    )
               ),
               
               ## Samples from countries //Maps     
               
               tabItem(
                    tabName = "country_map",
                    tabsetPanel(
                         type = "tabs",
                         tabPanel(
                              "EU+UK",
                              checkboxInput("relative_to_population_eu",
                                            label = span("Relative to population", bsButton("q_eu", label = "", icon = icon("info-circle"), style = "secondary", size = "small")),
                                            value = FALSE
                              ),
                              bsPopover(
                                   id = "q_eu", title = "Relative to population",
                                   content = paste0("If you check this box then sample numbers are divided by the size of the country population and multiplied <br> by 1 000 000"),
                                   trigger = "hover",
                                   options = list(container = "body")
                              ),
                              highchartOutput("eu_map", height = "600px")
                         ),
                         
                         tabPanel(
                              "World",
                              checkboxInput("relative_to_population_world",
                                            label = span("Relative to population", bsButton("q_world", label = "", icon = icon("info-circle"), style = "secondary", size = "small")),
                                            value = FALSE
                              ),
                              bsPopover(
                                   id = "q_world", title = "Relative to population",
                                   content = paste0("If you check this box then sample numbers are divided by the size of the country population and multiplied <br> by 1 000 000"),
                                   trigger = "hover",
                                   options = list(container = "body")
                              ),
                              highchartOutput("world_map", height = "600px")
                         )
                    ),
               ),      
               
               ## Variants       
               
               tabItem(
                    tabName = "variants",
                    fluidRow(
                         column(
                              width = 4,
                              selectInput("selected_country_for_variants",
                                          label = "Select country",
                                          choices = unique(variant_master_table$country),
                                          selected = "Netherlands"
                              ),
                              checkboxInput("include_ecdc_new_case",
                                            label = span(HTML('Include weekly new cases <a href = "https://coronavirus.jhu.edu/map.html">(JHU)</a>'), bsButton("q_variants", label = "", icon = icon("info-circle"), style = "secondary", size = "small")),
                                            value = FALSE
                              ),
                              bsPopover(
                                   id = "q_variants", title = "Include weekly new cases data from JHU",
                                   content = paste0("The newly diagnosed COVID-19 cases from each countries are reported by John Hopkins Coronavirus Resource Center. If you check this box then these numbers are also presented on the graph that gives information about how representative the sequenced dataset "),
                                   trigger = "hover",
                                   options = list(container = "body")
                              ),
                         ),
                         column(
                              width = 4,
                              radioSwitchButtons("vis_type",
                                                 label = "Type of visualization",
                                                 choices = c("absolute"="normal", "relative"="percent"),
                                                 selected = "normal",
                                                 selected_background = "#367fa9"
                              )
                         ),
                    ),
                    box(
                         status = "primary",
                         height = "650", width = "12", solidHeader = FALSE,
                         column(
                              width = 12,
                              highchartOutput("variant_weekly", height = "600px")
                         )
                    ),
               ),      
               
               ## Variants (VOC/VUI selection)      
               
               tabItem(
                    tabName = "lineage_graph_lineage",
                    fluidRow(
                         radioButtons("selected_lineage",
                                      label = "Select VOC/VOI",
                                      choices = variants,
                                      
                                      inline = TRUE, selected = unique(app_lineage$variant_id)[1]
                         ),
                         DT::dataTableOutput("table"),
                         box(
                              status = "primary",
                              height = "650", width = "12", solidHeader = FALSE,
                              column(
                                   width = 12,
                                   highchartOutput("distPlot_lineage_lineage", height = "600px"),
                              )
                         ),
                         box(
                              title = "Lineage specific mutations on open and closed state S protein structure",
                              footer = HTML('<p> Click on the 3D structures <a href="https://www.rcsb.org/structure/7A95"> (PDB: 7A95)</a>, and move your mouse to spin or use mouse wheel to zoom the protein for better view of the mutations</p>'),
                              status = "primary",
                              height = "520", width = "12", solidHeader = FALSE,
                              fluidRow(
                                   column(
                                        width = 6,
                                        NGLVieweROutput("structure_open")
                                   ),
                                   column(
                                        width = 6,
                                        NGLVieweROutput("structure_closed"),
                                   )
                              )
                         ),
                    ),
               ),
               
               ## Variants (Country selection)     
               
               tabItem(
                    tabName = "lineage_graph_country",
                    fluidRow(),
                    radioButtons("selected_country",
                                 label = "Select country",
                                 choices = unique(app_lineage$country),
                                 inline = TRUE, selected = "United Kingdom"
                    ),
                    box(
                         status = "primary",
                         height = "650", width = "12", solidHeader = FALSE,
                         column(
                              width = 12,
                              highchartOutput("distPlot_lineage_country", height = "600px")
                         )
                    ),
               ),
               
               
               ## Custom variant browser
               
               tabItem(
                    tabName = "custom_variant",
                    fluidRow(
                         # column(
                         #   width = 4,
                         #   # selectInput("selected_country_for_variants",
                         #   #             label = "Select country",
                         #   #             choices = unique(variant_master_table$country),
                         #   #             selected = "Netherlands"
                         #   # ),
                         # 
                         # 
                         # ),
                         
                         
                         box(width = "12",
                             
                             column(
                                  width = 9,
                                  strong("With the custom variant search it is posssible to select samples those contains or do not contains some predefined mutations"),
                                  checkboxInput("only_high_cov",
                                                label = span("Exclude samples with low sequencing depth (this may take several minutes)", bsButton("q_h_cov", label = "", icon = icon("info-circle"), style = "secondary", size = "small")),
                                                value = FALSE
                                  ),
                                  bsPopover(
                                       id = "q_h_cov", title = "Exclude samples with low sequencing depth (by clicking this the process may take longer eg. >10min",
                                       content = paste0("If you check this box then only those samples will be included where sequensing depth is higher than 30 by each selected positions"),
                                       trigger = "hover",
                                       options = list(container = "body")
                                  ),
                                  
                                  textInput("included_mutation_list",
                                            #label = h4("Included mutations"),
                                            label = span("Included mutations", bsButton("q_inlc_mut", label = "", icon = icon("info-circle"), style = "secondary", size = "small")),
                                            value = "D80A, D215G, K417N, E484K, N501Y, D614G, A701V"),
                                  bsPopover(
                                       id = "q_inlc_mut", title = "Included mutations",
                                       content = paste0("List of mutations in S protein that needs to be present in the sample. Use single letter amino acid signs and position in the S protein. Separate mutations with comma."),
                                       trigger = "hover",
                                       options = list(container = "body")
                                  ),
                                  textInput("excluded_mutation_list",
                                            #label = h4("Excluded mutations"),
                                            label = span("Excluded mutations", bsButton("q_excl_mut", label = "", icon = icon("info-circle"), style = "secondary", size = "small")),
                                            value = ""),
                                  bsPopover(
                                       id = "q_excl_mut", title = "Excluded mutations",
                                       content = paste0("List of mutations in S protein those are NOT present in the sample. Use single letter amino acid signs and position in the S protein. Separate mutations with comma."),
                                       trigger = "hover",
                                       options = list(container = "body")
                                  ),
                                  
                                  
                                  actionButton("run_custom_variant", "Run custom variant search", icon("refresh"), class = "btn btn-warning" )
                                  
                                  
                             ),
                             
                         ),
                         
                         box( width = "12",
                              uiOutput ("select_custom_country"),
                         ),
                         
                    ),
                    
                    
                    
                    
                    
                    # box(
                    #   status = "primary",
                    #   height = "650", width = "12", solidHeader = FALSE,
                    #   column(
                    #     width = 12,
                    #     highchartOutput("custom_variant_graph", height = "600px")
                    #   )
                    # ),
               ),      
               
               ## Info      
               tabItem(
                    tabName = "menu_info",
                    fluidRow(
                         textOutput("keepAlive"), # This need to stop fade away the app
                         tags$h3("Overview"), br(),
                         infoBoxOutput("version"), br(), br(), br(), br(), br(),
                         tags$h4("Number of samples"), br(),
                         infoBoxOutput("vcf_count"),
                         infoBoxOutput("meta_count"),  br(),
                         br(), br(), br(), br(), br(),'You can send any comments/suggestions: Kriszián Papp (krisztian.papp@phys-gs.elte.hu)', br(),
                    ),
               )
          ),
          
          ## Other
          # Note: the <script> below does not let the app fade away after 1 min
          tags$head(tags$style(HTML("
        /* navbar (rest of the header) */
        .skin-blue .main-header .navbar {
                              background-color: #ffffff;
                              } 
        /* logo */
        .skin-blue .main-header .logo {
                              background-color: #ffffff;
                              color: #357ca5;
                              font-weight: bold;
                              }                      
        /* logo when hovered */
        .skin-blue .main-header .logo:hover {
                              background-color: #ffffff;
                              color: #357ca5;
                              font-weight: bold;
                              }
        /* main sidebar */
        .skin-blue .main-sidebar {
                              background-color: #ffffff;
                              color: #4777ba;
                              }
         
         
        /* active selected tab in the sidebarmenu */
        .skin-blue .main-sidebar .sidebar .sidebar-menu .active a{
                              background-color: #ffffff;
                              color: #0c7abf;
                              font-weight: bold;
                              }                     
          
        /* other links in the sidebarmenu */
        .skin-blue .main-sidebar .sidebar .sidebar-menu a{
                              background-color: #ffffff;
                              color: #000000;
                              }
                              
                        
        /* other links in the sidebarmenu when hovered */
         .skin-blue .main-sidebar .sidebar .sidebar-menu a:hover{
                              background-color: #ffffff;
                              color: #0c7abf;
                              font-weight: bold;
                              }                
              
        /* toggle button when hovered  */                    
         .skin-blue .main-header .navbar .sidebar-toggle:hover{
                              background-color: #ffffff;
                              }
                              

           /* main section  */                    
         .skin-blue .content .btn_sm{
                              background-color: #ffffff;
         }
                              
                              
          <script>
          var socket_timeout_interval
          var n = 0
          $(document).on('shiny:connected', function(event) {
          socket_timeout_interval = setInterval(function(){
          Shiny.onInputChange('count', n++)
          }, 15000)
          });
          $(document).on('shiny:disconnected', function(event) {
          clearInterval(socket_timeout_interval)
          });
          </script>                    
                              
                              
                              ")
          )
          )
     )
)

############################################################################################ 
# Server part of the the app
############################################################################################

server <- function(input, output) {
     
     output$keepAlive <- renderText({
          req(input$count)
          paste("keep alive ", input$count)
     })     
     
     output$eu_map <- renderHighchart({
          map <- jsonlite::fromJSON(txt = "eugeo.json", simplifyVector = FALSE)
          if (input$relative_to_population_eu) {
               highchart() %>%
                    hc_add_series_map(
                         map, app_country_samples,
                         value = "relative_log_n_sample", joinBy = c("name", "country")
                    ) %>%
                    hc_legend(labelFormat = "", title = list(text = "Number of samples relative to 1 million citizen in log10 scale")) %>%
                    hc_title(text = "Number of raw SARS-CoV-2 sequence from EU+UK") %>%
                    hc_subtitle(text = "Move mouse above a country to see the numbers") %>%
                    hc_mapNavigation(enabled = TRUE) %>%
                    hc_tooltip(useHTML = TRUE, headerFormat = "", pointFormat = "{point.relative_n_sample} samples relative to 1 million derived from  {point.country}")%>%
                    hc_colors(colorstw)
          }
          else {
               highchart() %>%
                    hc_add_series_map(
                         map, app_country_samples,
                         value = "log_n_sample", joinBy = c("name", "country")
                    ) %>%
                    hc_legend(labelFormat = "", title = list(text = "Number of samples in log10 scale")) %>%
                    hc_title(text = "Number of raw SARS-CoV-2 sequence from EU+UK") %>%
                    hc_subtitle(text = "Move mouse above a country to see the numbers") %>%
                    hc_mapNavigation(enabled = TRUE) %>%
                    hc_tooltip(useHTML = TRUE, headerFormat = "", pointFormat = "{point.n_sample} samples derived from {point.country}")%>%
                    hc_colors(colorstw)
          }
     })
     
     
     output$world_map <- renderHighchart({
          worldgeojson$features[[3]]$properties$name <- "United States" # Fix non-standard country name in map file
          worldgeojson$features[[72]]$properties$name <- "Russian Federation" # Fix non-standard country name in map file
          if (input$relative_to_population_world) {
               highchart() %>%
                    hc_add_series_map(
                         worldgeojson, app_country_samples,
                         value = "relative_log_n_sample", joinBy = c("name", "country")
                    ) %>%
                    # hc_colorAxis(stops = color_stops()) %>%
                    hc_legend(labelFormat = "", title = list(text = "Number of samples relative to 1 million citizen in log10 scale")) %>%
                    hc_title(text = "Number of raw SARS-CoV-2 sequence from worldwide") %>%
                    hc_subtitle(text = "Move mouse above the country to see the numbers") %>%
                    hc_mapNavigation(enabled = TRUE) %>%
                    hc_tooltip(useHTML = TRUE, headerFormat = "", pointFormat = "{point.relative_n_sample} samples relative to 1 million derived from {point.country}")%>%
                    hc_colors(colorstw)
          }
          else {
               highchart() %>%
                    hc_add_series_map(
                         worldgeojson, app_country_samples,
                         value = "log_n_sample", joinBy = c("name", "country")
                    ) %>%
                    hc_legend(labelFormat = "", title = list(text = "Number of samples in log10 scale")) %>%
                    hc_title(text = "Number of raw SARS-CoV-2 sequence from worldwide") %>%
                    hc_subtitle(text = "Move mouse above the country to see the numbers") %>%
                    hc_mapNavigation(enabled = TRUE) %>%
                    hc_tooltip(useHTML = TRUE, headerFormat = "", pointFormat = "{point.n_sample} samples derived from {point.country}")%>%
                    hc_colors(colorstw)
          }
     })
     
     
     output$structure_open <- renderNGLVieweR({
          selected_AA <- lineage_def_data %>%
               dplyr::filter(variant_id == !!input$selected_lineage) %>%
               dplyr::filter(
                    gene == "S"
               ) %>%
               dplyr::select("protein_codon_position") %>%
               collect()
          y <- paste(as.character(selected_AA$protein_codon_position), "OR", collapse = " OR ")
          x <- paste("(:A) AND ( ", y, " )", sep = "")
          
          NGLVieweR("7A95") %>%
               addRepresentation("cartoon", param = list(name = "S1", color = "brown", sele = ":A")) %>%
               addRepresentation("cartoon", param = list(name = "ACE2", color = "darkblue", sele = ":D")) %>%
               addRepresentation("cartoon", param = list(name = "S2", color = "lightgray", sele = ":B")) %>%
               addRepresentation("cartoon", param = list(name = "S3", color = "lightgray", sele = ":C")) %>%
               addRepresentation("label", param = list(
                    sele = ":D AND 608",
                    labelType = "format",
                    labelFormat = "ACE2", # or enter custom text
                    labelGrouping = "residue", # or "atom" (eg. sele = "20:A.CB")
                    color = "white",
                    fontFamiliy = "sans-serif",
                    xOffset = 1,
                    yOffset = 0,
                    zOffset = 0,
                    fixedSize = TRUE,
                    radiusType = 1,
                    radiusSize = 2, # Label size
                    showBackground = TRUE,
                    backgroundColor = "darkblue",
                    backgroundOpacity = 1
               )) %>%
               addRepresentation("label", param = list(
                    sele = x,
                    labelType = "format",
                    labelFormat = "[%(resname)s]%(resno)s", # or enter custom text
                    labelGrouping = "residue", # or "atom" (eg. sele = "20:A.CB")
                    color = "black",
                    fontFamiliy = "sans-serif",
                    xOffset = 1,
                    yOffset = 0,
                    zOffset = 0,
                    fixedSize = TRUE,
                    radiusType = 1,
                    radiusSize = 1.5, # Label size
                    showBackground = FALSE
               )) %>%
               addRepresentation("label", param = list(
                    sele = ":A AND 1146",
                    labelType = "format",
                    labelFormat = "S (open state)", # or enter custom text
                    labelGrouping = "residue", # or "atom" (eg. sele = "20:A.CB")
                    color = "white",
                    fontFamiliy = "sans-serif",
                    xOffset = 20,
                    yOffset = 20,
                    zOffset = 0,
                    fixedSize = TRUE,
                    radiusType = 1,
                    radiusSize = 2, # Label size
                    showBackground = TRUE,
                    backgroundColor = "brown",
                    backgroundOpacity = 1
               )) %>%
               stageParameters(backgroundColor = "white", zoomSpeed = 1) %>%
               addRepresentation("surface", param = list(name = "surface", colorValue = c("chartreuse"), sele = x))
     })
     
     
     output$structure_closed <- renderNGLVieweR({
          selected_AA <- lineage_def_data %>%
               dplyr::filter(variant_id == !!input$selected_lineage) %>%
               dplyr::filter(
                    gene == "S"
               ) %>%
               dplyr::select("protein_codon_position") %>%
               collect()
          y <- paste(as.character(selected_AA$protein_codon_position), "OR", collapse = " OR ")
          x <- paste("(:B) AND ( ", y, " )", sep = "")
          
          NGLVieweR("7A95") %>%
               addRepresentation("cartoon", param = list(name = "S1", color = "lightgray", sele = ":A")) %>%
               addRepresentation("cartoon", param = list(name = "ACE2", color = "lightgray", sele = ":D")) %>%
               addRepresentation("cartoon", param = list(name = "S2", color = "orange", sele = ":B")) %>%
               addRepresentation("cartoon", param = list(name = "S3", color = "lightgray", sele = ":C")) %>%
               addRepresentation("label", param = list(
                    sele = x,
                    labelType = "format",
                    labelFormat = "[%(resname)s]%(resno)s", # or enter custom text
                    labelGrouping = "residue", # or "atom" (eg. sele = "20:A.CB")
                    color = "black",
                    fontFamiliy = "sans-serif",
                    xOffset = 1,
                    yOffset = 0,
                    zOffset = 0,
                    fixedSize = TRUE,
                    radiusType = 1,
                    radiusSize = 1.5, # Label size
                    showBackground = FALSE
               )) %>%
               addRepresentation("label", param = list(
                    sele = ":B AND 1146",
                    labelType = "format",
                    labelFormat = "S (closed state)", # or enter custom text
                    labelGrouping = "residue", # or "atom" (eg. sele = "20:A.CB")
                    color = "white",
                    fontFamiliy = "sans-serif",
                    xOffset = 20,
                    yOffset = 20,
                    zOffset = 0,
                    fixedSize = TRUE,
                    radiusType = 1,
                    radiusSize = 2, # Label size
                    showBackground = TRUE,
                    backgroundColor = "orange",
                    backgroundOpacity = 1
               )) %>%
               stageParameters(backgroundColor = "white", zoomSpeed = 1) %>%
               addRepresentation("surface", param = list(name = "surface", colorValue = c("chartreuse"), sele = x))
     })
     
     
     output$euPlot <- renderHighchart({
          eu <- c(
               "United Kingdom", "Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus", "Czech Republic", "Denmark", "Estonia", "Finland", "France", "Germany", "Greece", "Hungary",
               "Ireland", "Italy", "Latvia", "Lithuania", "Luxembourg", "Malta", "Netherlands", "Poland", "Portugal", "Romania", "Slovakia", "Slovenia", "Spain", "Sweden"
          )
          x <- app_human_meta_mv %>%
               dplyr::filter(country_name %in% local(eu)) %>%
               group_by(country_name) %>%
               arrange(date) %>%
               mutate(sum_weekly_sample = cumsum(weekly_sample)) %>%
               ungroup()
          if (input$eu_graph_type=="Weekly sample number"){
               highchart(type = "stock") %>%
                    hc_add_series(x, "scatter", hcaes(x = date, y = weekly_sample, group = country_name),
                                  tooltip = list(pointFormat = "Number of samples sequenced on a given week in {point.Country}:{point.weekly_sample}: ")
                    ) %>%
                    hc_title(text = "Number of samples derived from EU+UK states on a given week") %>%
                    hc_legend(
                         enabled = TRUE,
                         title = list(text = "Click below on countries to hide/show them on the graph:")
                    ) %>%
                    hc_yAxis(
                         title = list(text = "Number of samples"),
                         min = 1,
                         type = input$eu_log_lin,
                         minorTickInterval = "auto"
                    ) %>%
                    hc_tooltip(crosshairs = TRUE) %>%
                    hc_navigator(enabled = FALSE) %>%
                    hc_scrollbar(enabled = FALSE) %>%
                    hc_plotOptions(scatter = list(lineWidth = 1))%>%
                    hc_colors(colorstw)
          } else {
               if (input$eu_graph_type=="Cumulative sample number"){
                    highchart(type = "stock") %>%
                         hc_add_series(x, "scatter", hcaes(x = date, y = sum_weekly_sample, group = country_name),
                                       tooltip = list(pointFormat = "Number of samples sequenced in {point.Country}:{point.sum_weekly_sample}: ")
                         ) %>%
                         hc_title(text = "Number of samples derived from EU+UK states on a given week") %>%
                         hc_legend(
                              enabled = TRUE,
                              title = list(text = "Click below on countries to hide/show them on the graph:")
                         ) %>%
                         hc_yAxis(
                              title = list(text = "Number of samples"),
                              min = 1,
                              type = input$eu_log_lin,
                              minorTickInterval = "auto"
                         ) %>%
                         hc_tooltip(crosshairs = TRUE) %>%
                         hc_navigator(enabled = FALSE) %>%
                         hc_scrollbar(enabled = FALSE) %>%
                         hc_plotOptions(scatter = list(lineWidth = 1))%>%
                         hc_colors(colorstw)
               } else {
                    x <- app_human_meta_mv_jhd %>%
                         dplyr::filter(country_name %in% local(eu))%>%
                         dplyr::filter(pct>=0)
                    highchart(type = "stock") %>%
                         hc_add_series(x, "scatter", hcaes(x = date, y = pct, group = country_name),
                                       tooltip = list(pointFormat = "Percent of sequenced new cases <br> on a given week in {point.country_name}: {point.pct} % ")
                         ) %>%
                         hc_title(text = "Percent of sequenced new cases on a given week") %>%
                         hc_legend(
                              enabled = TRUE,
                              title = list(text = "Click below to countries to hide/show them on the graph:")
                         ) %>%
                         hc_yAxis(
                              title = list(text = "Analysed sample / new cases * 100"),
                              type = input$eu_log_lin
                              #min = 0
                         ) %>%
                         hc_tooltip(crosshairs = TRUE) %>%
                         hc_navigator(enabled = FALSE) %>%
                         hc_scrollbar(enabled = FALSE)%>%
                         hc_colors(colorstw)
               }
          }
     })
     
     
     output$worldPlot <- renderHighchart({
          x <- app_human_meta_mv %>%
               dplyr::filter(country_name %in% input$world_select_country) %>%
               group_by(country_name) %>%
               arrange(date) %>%
               mutate(sum_weekly_sample = cumsum(weekly_sample)) %>%
               ungroup()
          
          if (input$world_graph_type=="Weekly sample number"){
               highchart(type = "stock") %>%
                    hc_add_series(x, "scatter", hcaes(x = date, y = weekly_sample, group = country_name),
                                  tooltip = list(pointFormat = "Number of samples sequenced on a given week in {point.Country}:{point.weekly_sample}: ")
                    ) %>%
                    hc_title(text = "Number of samples derived from countries on a given week") %>%
                    hc_legend(
                         enabled = TRUE,
                         title = list(text = "Click below on countries to hide/show them on the graph:")
                    ) %>%
                    hc_yAxis(
                         title = list(text = "Number of samples"),
                         min = 1,
                         type = input$world_log_lin,
                         minorTickInterval = "auto"
                    ) %>%
                    hc_tooltip(crosshairs = TRUE) %>%
                    hc_navigator(enabled = FALSE) %>%
                    hc_scrollbar(enabled = FALSE) %>%
                    hc_plotOptions(scatter = list(lineWidth = 1))%>%
                    hc_colors(colorstw)
          } else {
               if (input$world_graph_type=="Cumulative sample number"){
                    highchart(type = "stock") %>%
                         hc_add_series(x, "scatter", hcaes(x = date, y = sum_weekly_sample, group = country_name),
                                       tooltip = list(pointFormat = "Number of samples sequenced in {point.Country}:{point.sum_weekly_sample}: ")
                         ) %>%
                         hc_title(text = "Number of samples derived from countries on a given week") %>%
                         hc_legend(
                              enabled = TRUE,
                              title = list(text = "Click below on countries to hide/show them on the graph:")
                         ) %>%
                         hc_yAxis(
                              title = list(text = "Number of samples"),
                              min = 1,
                              type = input$world_log_lin,
                              minorTickInterval = "auto"
                         ) %>%
                         hc_tooltip(crosshairs = TRUE) %>%
                         hc_navigator(enabled = FALSE) %>%
                         hc_scrollbar(enabled = FALSE) %>%
                         hc_plotOptions(scatter = list(lineWidth = 1))%>%
                         hc_colors(colorstw)
               } else {
                    x <- app_human_meta_mv_jhd %>%
                         dplyr::filter(pct>=0)%>%
                         dplyr::filter(country_name %in% input$world_select_country)
                    highchart(type = "stock") %>%
                         hc_add_series(x, "scatter", hcaes(x = date, y = pct, group = country_name),
                                       tooltip = list(pointFormat = "Percent of sequenced new cases <br> on a given week in {point.country_name}: {point.pct} % ")
                         ) %>%
                         hc_title(text = "Percent of sequenced new cases on a given week") %>%
                         hc_legend(
                              enabled = TRUE,
                              title = list(text = "Click below to countries to hide/show them on the graph:")
                         ) %>%
                         hc_yAxis(
                              title = list(text = "Analysed sample / new cases * 100"),
                              type = input$world_log_lin
                              #min = 0
                         ) %>%
                         hc_tooltip(crosshairs = TRUE) %>%
                         hc_navigator(enabled = FALSE) %>%
                         hc_scrollbar(enabled = FALSE)%>%
                         hc_colors(colorstw)
               }
          }
     })
     
     
     output$vcf_count <- renderInfoBox({
          infoBox(
               title = "Analyzed", value = as.character(unique_ena_run_summary[unique_ena_run_summary$table_name=="vcf", "count"]),
               icon = icon("circle"),
               color = "yellow"
          )
     })
     
     
     output$meta_count <- renderInfoBox({
          infoBox(
               title = "Submitted", value = as.character(unique_ena_run_summary[unique_ena_run_summary$table_name=="meta", "count"]),
               icon = icon("circle"),
               color = "yellow"
          )
     })
     
     
     output$version <- renderInfoBox({
          infoBox(
               title = "App version", value = app_version,
               icon = icon("circle"),
               color = "blue"
          )
     })
     
     
     output$distPlot_lineage_country <- renderHighchart({
          x <- app_lineage %>%
               dplyr::filter(country == input$selected_country) %>%
               arrange(collection_date)
          highchart(type = "stock") %>%
               hc_add_series(x, "scatter", hcaes(x = collection_date, y = pct, group = variant_id),
                             tooltip = list(pointFormat = "{point.n} samples from {point.variant_id} variant out of {point.n_all} ")
               ) %>%
               hc_title(text = "Percent of samples derived from a given variant detected in the countries") %>%
               hc_legend(
                    enabled = TRUE,
                    title = list(text = "Click any of the varints below to show/hide them on the graph:")
               ) %>%
               hc_yAxis(title = list(text = "Percent of samples")) %>%
               hc_tooltip(crosshairs = TRUE) %>%
               hc_navigator(enabled = FALSE) %>%
               hc_scrollbar(enabled = FALSE) %>%
               hc_colors(colorstw)
     })
     
     
     output$distPlot_lineage_lineage <- renderHighchart({
          x <- app_lineage %>%
               dplyr::filter(variant_id == input$selected_lineage) %>%
               arrange(collection_date)
          highchart(type = "stock") %>%
               hc_add_series(x, "scatter", hcaes(x = collection_date, y = n, group = country),
                             tooltip = list(pointFormat = "Number of samples {point.n}: ")
               ) %>%
               hc_title(text = "Number of samples from a given variant detected in the countries") %>%
               hc_legend(
                    enabled = TRUE,
                    title = list(text = "Click any of the countries below to show/hide them on the graph:")
               ) %>%
               hc_yAxis(title = list(text = "Number of samples")) %>%
               hc_tooltip(crosshairs = TRUE) %>%
               hc_navigator(enabled = FALSE) %>%
               hc_scrollbar(enabled = FALSE) %>%
               hc_colors(colorstw)
     })
     
     
     output$variant_weekly <- renderHighchart({
          x <- variant_master_table %>%
               dplyr::filter(country == input$selected_country_for_variants) %>%
               dplyr::arrange(date)
          if (!input$include_ecdc_new_case) x <- dplyr::filter(x, name != "Non-sequenced new cases")
          if (input$vis_type == "normal") yaxis_title <- "Number of samples" else yaxis_title <- "Percent"
          highchart() %>%
               hc_chart(type = "column") %>%
               hc_title(text = paste("Weekly cases in ", input$selected_country_for_variants, sep = "")) %>%
               hc_subtitle(text = "Move mouse above columns to see the exact number of cases on given week") %>%
               hc_plotOptions(column = list(
                    dataLabels = list(enabled = FALSE),
                    stacking = input$vis_type,
                    enableMouseTracking = TRUE
               )) %>%
               hc_yAxis(title = list(text = yaxis_title)) %>%
               hc_legend(
                    enabled = TRUE,
                    title = list(text = "Click any of the varints below to show/hide them on the graph:")
               ) %>%
               hc_tooltip(split = TRUE) %>%
               hc_add_series(x, "column", hcaes(x = str_c(date_year, " ", date_week, ". week", sep = ""), y = value, group = name)) %>%
               hc_xAxis(categories = unique(x$date))%>%
               hc_colors(colorstw)
     })

     
     
     custom_variant_tables <- eventReactive(input$run_custom_variant, {
          incl <- convert_mut_1_to_3(input$included_mutation_list)
          excl <- convert_mut_1_to_3(input$excluded_mutation_list)
          
          #incl <- strsplit(str_remove_all(input$included_mutation_list, pattern = " "), ",")[[1]]
          #excl <- strsplit(str_remove_all(input$excluded_mutation_list, pattern = " "), ",")[[1]]
          if (is_empty(incl)) incl <- c("p.Asp80Ala", "p.Asp215Gly", "p.Lys417Asn", "p.Glu484Lys", "p.Asn501Tyr", "p.Asp614Gly", "p.Ala701Val")
          if (is_empty(excl)) excl <- ""
          #x_all <- collect_selected_variant(con, variants)
          #x <- collect_selected_variant(con, variants = incl, exclusion = excl)
          incl <- str_c(incl, collapse = ",")
          excl <- str_c(excl, collapse = ",")
          
          #if (input$only_high_cov) x <- collect_selected_variant_with_cov(con, variants = incl, exclusion = excl) else x <- collect_selected_variant(con, variants = incl, exclusion = excl)
          
          if (input$only_high_cov) {
               param <- str_c("?limit=10000000&included=", incl, "&excluded=", excl, collapse = "")
               x <- list(table1 = d.load_custom(url = url, endpoint = "filter_custom_browser_cov", param = param),
                         table2 = d.load_custom(url = url, endpoint = "filter_custom_browser_cov_time", param = param))
          }
          
          # This part below is wrong!
          else {
               x <-list(table1 = d.load_custom(url = url, endpoint = "filter_custom_browser_cov", param = param),
                        table2 = d.load_custom(url = url, endpoint = "filter_custom_browser_cov_time", param = param))
               }
          
          
         
          x
     })
     
     
     
     output$select_custom_country <- renderUI({
          
          if  (!is_empty(custom_variant_tables())) {
               #d_chose <- custom_variant_tables()$table1$country
               #  
               # tagList(
               #   pickerInput ("selected_country_c", "Select country:",
               #                                 choices = d_chose,
               #                                multiple = FALSE,
               #                                 selected = d_chose[1])
               # )
               
               column(
                    width = 12,
                    box( title = "Countries with samples of custom variant",
                         status = "primary",
                         height = "500", width = "12",solidHeader = T,
                         column(width = 4,
                                DT::dataTableOutput("custom_variant_table"),
                                style = "height:440px; overflow-y: scroll;"
                         ),
                         column(
                              width = 8,
                              highchartOutput("custom_variant_graph", height = "440px")
                         )
                    )
               )
               
               # column(
               #   width = 8,
               # 
               # box(
               #   status = "primary",
               #   height = "200", width = "12", solidHeader = FALSE,
               #   column(
               #     width = 12,
               #     highchartOutput("custom_variant_graph", height = "200px")
               #   )
               # )
               # )
               
               
               
               
               
               
               
          }
          
          # if  (!is_empty(custom_variant_tables())) {
          #   d_chose <- custom_variant_tables()$table1
          #   d_chose <- as.character(unique(d_chose$country))
          #   textOutput({renderText("last_price")})
          #    pickerInput ("selected_country_c", "Select country:",
          #                 choices = d_chose,
          #                 multiple = FALSE,
          #                 selected = d_chose[1])
          # }
          #  
          
          #d_chose <- custom_variant_tables()
          #d_chose <- d_chose[[1]]
          #d_chose <- as.character(unique(d_chose[[1]]$country))
          # pickerInput ("selected_country_c", "Select country:",
          #              choices = d_chose,
          #              multiple = FALSE,
          #              selected = d_chose[1])
          
          
          
          # DT::renderDataTable(
          #   DT::datatable(
          #     { data.frame(x= c("a", "b"),
          #                  y = c(1,2))
          #     },
          #     #selection = 'single',
          #     #extensions = 'Buttons',
          #     options = list(lengthChange = FALSE,
          #                    paging = FALSE,
          #                    dom = 't')
          #   )
          # )
          
          
          #haha <- c("lshfldshfkdhsf")
          
          
          # div(
          #   tags$b("Last Traded Price")
          # )
          #textOutput(input$included_mutation_list, "HAHA")
          
          
     })
     # 
     
     
     
     
     
     # output$custom_variant_graph <- renderHighchart({
     #   
     #   variants <- c("D80A"="p.Asp80Ala",
     #                 "D215G"="p.Asp215Gly",
     #                 #"LAL242-244del"="p.Ala243_Leu244del", #seems like these variants are not called correctly
     #                 #"R246I"="p.Arg246Ile", #seems like these variants are not called correctly
     #                 "K417N"="p.Lys417Asn",
     #                 "E484K"="p.Glu484Lys",
     #                 "N501Y"="p.Asn501Tyr",
     #                 "D614G"="p.Asp614Gly",
     #                 "A701V"="p.Ala701Val")
     #   exlcusion <- c("")
     #   
     # 
     #   
     #   x_all <- collect_selected_variant(con, variants)
     #   
     #   x <- x_all[[2]] %>%
     #     drop_na() %>%
     #     dplyr::filter(country=="Finland") %>%
     #     rename(`Other variant` = "other_count",
     #            `Selected variant` = "variant_count") %>%
     #     pivot_longer(cols = c("Other variant", "Selected variant"))
     #   x$collection_date <- as.Date(x$collection_date)
     # 
     #   highchart() %>%
     #     hc_chart(type = "column") %>%
     #     #hc_title(text = paste("Weekly cases in ", input$selected_country_for_variants, sep = "")) %>%
     #     hc_subtitle(text = "Move mouse above columns to see the exact number of cases on given week") %>%
     #     # hc_plotOptions(column = list(
     #     #   dataLabels = list(enabled = FALSE),
     #     #   stacking = input$vis_type,
     #     #   enableMouseTracking = TRUE
     #     # )) %>%
     #     #hc_yAxis(title = list(text = yaxis_title)) %>%
     #     hc_legend(
     #       enabled = TRUE,
     #       title = list(text = "Click any of the varints below to show/hide them on the graph:")
     #     ) %>%
     #     hc_tooltip(split = TRUE) %>%
     #     hc_add_series(x, "column", hcaes(x = collection_date, y = value, group = name)) %>%
     #     #hc_xAxis(categories = unique(x$collection_date))%>%
     #     hc_xAxis(dateTimeLabelFormats = list(day = '%m  %Y'), type = "datetime") %>%
     #     hc_colors(colorstw)
     # })
     # 
     
     
     
     output$custom_variant_table <- DT::renderDataTable(
          DT::datatable(
               { custom_variant_tables()$table1 %>%
                         arrange(desc(count))
               },
               colnames = c('Country', 'Count'),
               selection = 'single',
               options = list(lengthChange = FALSE,
                              paging = FALSE,
                              dom = 't')
          )
     ) 
     
     
     
     
     
     output$custom_variant_graph <- renderHighchart({
          if  (!is_empty(custom_variant_tables())) {
               s <- input$custom_variant_table_rows_selected
               if (!length(s)) s = 1
               y <- custom_variant_tables()$table1  %>%
                    arrange(desc(count))
               
               ss <- as.character(y[s, 'country'])
               if (!length(s)) 
               {x <- tibble(country = character(),
                            collection_date = date(),
                            value = numeric(),
                            group = character())
               } else {
                    
                    x <- custom_variant_tables()$table2 %>%
                         drop_na() %>%
                         dplyr::filter(country==ss) %>%
                         rename(`Other variant` = "other_count",
                                `Selected variant` = "variant_count") %>%
                         pivot_longer(cols = c("Other variant", "Selected variant"))
                    x$collection_date <- as.Date(x$collection_date)
               }
               
               highchart() %>%
                    hc_chart(type = "column") %>%
                    #hc_title(text = paste("Weekly cases in ", input$selected_country_for_variants, sep = "")) %>%
                    hc_title(text = ss) %>%
                    # hc_plotOptions(column = list(
                    #   dataLabels = list(enabled = FALSE),
                    #   stacking = input$vis_type,
                    #   enableMouseTracking = TRUE
                    # )) %>%
                    #hc_yAxis(title = list(text = yaxis_title)) %>%
                    hc_legend(
                         enabled = TRUE,
                         title = list(text = "Click any of the varints below to show/hide them on the graph:")
                    ) %>%
                    hc_tooltip(split = TRUE) %>%
                    hc_add_series(x, "column", hcaes(x = collection_date, y = value, group = name)) %>%
                    #hc_xAxis(categories = unique(x$collection_date))%>%
                    hc_xAxis(dateTimeLabelFormats = list(day = '%m  %Y'), type = "datetime") %>%
                    hc_colors(colorstw)
          }
     })
     
     
     
     
     
     
     
     output$table <- DT::renderDataTable(
          DT::datatable(
               {
                    lineage_def_data %>%
                         distinct (variant_id, .keep_all = TRUE) %>%
                         select (variant_id, pango, description) %>%
                         dplyr::filter(variant_id == input$selected_lineage)
               },
               options = list(
                    lengthChange = FALSE,
                    paging = FALSE,
                    dom = "t"
               )
          ),
     )
}

# Run the application
shinyApp(ui = ui, server = server)
