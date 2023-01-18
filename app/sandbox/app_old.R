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
library(shinybusy)

app_version <- "v_003.005"

# Connection details


config <- config::get()
con <- dbPool(
  drv = RPostgreSQL::PostgreSQL(),
  dbname = config$dbname,
  host = config$host,
  port = config$port,
  user = config$user,
  password = config$password,
  option = config$option
)

onStop(function() {
  poolClose(con)
})

# Configuration
colorstw <- c(brewer.pal(n=8, name="Set2"),brewer.pal(n=12, name="Paired")[-c(3,7)], "#575858", "#c7f89c")[1:19]

# Variables from database

app_country_samples <- tbl(con, "app_country_samples_full") %>%
  collect()%>%
  as.data.frame() 

app_lineage <- tbl(con, "app_lineage") %>%
  collect()

# app_new_cases <- tbl(con, "app_new_cases") %>%
#   collect()

app_human_meta_mv <- tbl(con, "app_human_meta_mv") %>%
  collect()

app_human_meta_mv_jhd <- tbl(con, "app_human_meta_mv_jhd") %>%
  collect()

app_new_cases <- tbl(con, "app_new_cases_jhd") %>%
  collect()

app_variants_weekly <- tbl(con, "app_variants_weekly") %>%
  collect()

# app_worldplot_data<- tbl(con, "app_worldplot_data") %>%
#   collect()

lineage_def_data <- tbl(con, "lineage_def") %>%
  collect()

unique_ena_run_summary <- tbl(con, "unique_ena_run_summary") %>%
  collect()



#sample_count <- tbl(con, "app_sample_count") %>% 
#  collect()

### This part will create the master table for variant graphs

## Ez a rész már be lett doblva a materialized viewba
# new_cases <- app_new_cases %>%
#   mutate(date_week_iso = ifelse(date_week < 10, str_c("0", as.character(date_week)), as.character(date_week))) %>%
#   mutate(date = ISOweek2date(paste(date_year, "-W", date_week_iso, "-1", sep = ""))) %>%
#   dplyr::select(country, date, date_year, date_week, weekly_sample, cases)
### eddig

# ezt a pici részt ki lehet venni, ha frissül a mat view
# new_cases <- app_new_cases %>%
#    mutate(date_week_iso = ifelse(date_week < 10, str_c("0", as.character(date_week)), as.character(date_week))) %>%
#    mutate(date = paste(as.character(date_year), "-W", as.character(date_week_iso), "-1", sep = "")) %>%
#    mutate(date = ISOweek2date(date)) %>%
#    dplyr::select(country, date, date_year, date_week, weekly_sample, cases)

variants_weekly <- app_variants_weekly %>%
  pivot_wider(names_from = variant_id, values_from = weekly_variant_sample)

variants_weekly$cases_with_variant_id <- rowSums(variants_weekly[, c(-1, -2, -3)], na.rm = TRUE)


variant_master_table <- app_new_cases %>%
  left_join(variants_weekly) %>%
  mutate(
    # `Other variants` = weekly_sample - cases_with_variant_id,
    `Non-sequenced new cases` = cases - weekly_sample
  ) %>%
  dplyr::select(-weekly_sample, -cases, -cases_with_variant_id) %>%
  pivot_longer(cols = !any_of(c("country", "date", "date_year", "date_week")))

variants <- unique(app_lineage$variant_id)
variants <- variants[variants!="Not analysed yet " & variants!="Other variant" ]

# This part creates a table for world data plot

# 
# worldplot_data <- app_worldplot_data %>%
#   mutate(date_week_iso = ifelse(date_week < 10, str_c("0", as.character(date_week)), as.character(date_week))) %>%
#   mutate(date = ISOweek2date(paste(date_year, "-W", date_week_iso, "-1", sep = ""))) %>%
#   group_by(country) %>%
#   arrange(date) %>%
#   mutate(sum_weekly_sample = cumsum(weekly_sample)) %>%
#   ungroup()


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

# table_description <- tbl(con, "table_description") %>%
#   collect()
# inds <- vector()
# for (i in 1:nrow(table_description)){
#   inds[i] <- paste(table_description[i,"type"], table_description[i,"table_name"], sep = '/')
# }
# 
# nodes <- makeNodes(inds)


############################################################################################
# User interface of the app
############################################################################################

ui <- dashboardPage(
  
  dashboardHeader(
    title = "CoVEO"
    # tags$li(p(
    #   a(
    #     href = "https://www.veo-europe.eu/",
    #     img(
    #       src = "veo_logo.png",
    #       title = "Versatile emerging infectious disease observatory - VEO", height = "40px"
    #     ),
    #     style = "padding-top:10px; padding-bottom:10px;"
    #   ),
    #   a(
    #     href = "https://www.covid19dataportal.org/",
    #     img(
    #       src = "data_portal_logo.png",
    #       title = "COVID-19 Data Portal", height = "40px"
    #     ),
    #     style = "padding-top:10px; padding-bottom:10px;"
    #   )
    # ),
    # class = "dropdown"
    # )
  ),
  
  
  
  dashboardSidebar(
    sidebarMenu(
      id = "sidebar",
      menuItem("Samples from countries",
               icon = icon("map-marked-alt"), startExpanded = TRUE,
               menuSubItem("Graphs", tabName = "country_graph"),
               menuSubItem("Maps", tabName = "country_map")
               
      ),
      # menuItem("Samples from countries (graphs)", tabName = "country_graph", icon = icon("chart-bar")),
      menuItem("Variants", tabName = "variants", icon = icon("chart-bar")),
      menuItem("Variants (VOC/VUI selection)", tabName = "lineage_graph_lineage", icon = icon("chart-bar")),
      menuItem("Variants (Country selection)", tabName = "lineage_graph_country", icon = icon("chart-bar")),
      #menuItem("Demo notebooks", tabName = "demo_notebook", icon = icon("table")),
      menuItem("Info", tabName = "menu_info", icon = icon("info"))
    )
  ),
  
  
  
  dashboardBody(
    add_busy_spinner(spin = "fading-circle", position = "bottom-right", timeout = 1000),
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
            # tags$h4("The graph below shows how many samples were sequenced and sent to EBI in a gived day in a given EU state"),
            #
            
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
              height = "450", width = "12", solidHeader = FALSE,
              column(
                width = 12,
                highchartOutput("euPlot"),
              )
            )
          ),
          
          tabPanel(
            "World",
            # tags$h4("The graph below shows how many samples were sequenced and sent to EBI in a gived day in a given EU state"),
            #
            
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
              height = "450", width = "12", solidHeader = FALSE,
              column(
                width = 12,
                highchartOutput("worldPlot"),
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
            # tags$h4("The map below shows how many sequenced samples arrived from various EU state"),
            # checkboxInput("relative_to_population_eu", label = "Relative to population", value = FALSE),
            
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
            # tags$h4("The graph below shows how many samples were sequenced and sent to EBI in a gived day in a given EU state"),
            # checkboxInput("relative_to_population_world", label = "Relative to population"),
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
                          label = span(HTML('Include weekly new cases <a href = "https://www.ecdc.europa.eu/en/publications-data/data-national-14-day-notification-rate-covid-19">(ECDC)</a>'), bsButton("q_variants", label = "", icon = icon("info-circle"), style = "secondary", size = "small")),
                          value = FALSE
            ),
            
            
            bsPopover(
              id = "q_variants", title = "Include weekly new cases data from ECDC",
              content = paste0("The newly diagnosed COVID-19 cases from each countries are reported weekly by ECDC. If you check this box then these numbers are also presented on the graph that gives information about how representative the sequenced dataset "),
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
          height = "450", width = "12", solidHeader = FALSE,
          column(
            width = 12,
            highchartOutput("variant_weekly")
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
            height = "450", width = "12", solidHeader = FALSE,
            column(
              width = 12,
              highchartOutput("distPlot_lineage_lineage"),
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
      
      
      
      # tabItem(
      #   tabName = "demo_notebook",
      #   fluidRow(
      #     "These are demo notebooks that show examples how to reach/query the database.", br(),
      #     tags$li(tags$a(href = "https://veo.vo.elte.hu/report/vcfdatabasewithrdemo/vcf_database_demo_v04_20210504.nb.html", "How to reach the databse from R")),
      #     tags$li(tags$a(href = "https://veo.vo.elte.hu/report/vcfdatabasewithpythondemo/vcf_database_demo_python_v03_210628%20(1).html", "How to reach the databse from Python")),
      #     "These are usefull examples that can help to make your own notebook.", br(),
      #     'If you need help, then do not hesitate to contact us: "krisztian.papp@phys-gs.elte.hu", we waiting for any comments/suggestions', br(),
      #   ),
      # ),
      
       
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
          height = "450", width = "12", solidHeader = FALSE,
          column(
            width = 12,
            highchartOutput("distPlot_lineage_country")
          )
        ),
      ),
      
      
      
## Info      
      tabItem(
        tabName = "menu_info",
        fluidRow(
          tags$h3("Overview"), br(),
          # tags$img(src='vcf_database_overview.png'),
          
          infoBoxOutput("version"), br(), br(), br(), br(), br(),
          tags$h4("Number of samples"), br(),
          infoBoxOutput("vcf_count"),
          infoBoxOutput("meta_count"),  br(),
          
          
          # box( title = "Schema Browser",
          #      status = "primary",
          #      width = "4",solidHeader = T,
          #      column(width = 12,
          #             jstreeOutput("jstree"),
          #             style = "height:380px; overflow-y: scroll;overflow-x: scroll;"
          #      ),
          #      
          # ),
          # 
          # 
          # box(
          #   title = 'Description of the selected table/view/function',
          #   status = "primary",
          #   width = "8", solidHeader = TRUE,
          #   h3(textOutput("filtered_table_name")),
          #   h4(textOutput("filtered_table_title")),
          #   textOutput("filtered_table_description"),
          #   
          #   
          #   column(
          #     width = 12,
          #     #textOutput("textxxx"),
          #     DT::dataTableOutput("table_selected_column_description"),
          #     style = "overflow-x: scroll;"
          #   ),
          #   verbatimTextOutput("filtered_table_sql")
          # ),
          
          
          
          # box(
          #   title = 'Table "meta":',
          #   status = "primary",
          #   width = "12", solidHeader = TRUE,
          #   "Contains the metadata for each ENA run accession number", br(), "A few extra column was inserted",
          #   tags$li('"clean_counry" column contains only country names (the original "country" column sometimes also contains smaller region names, not only country name)'),
          #   tags$li('"clean_host" column contains "Homo sapiens" if sample derived from human host (this is a cleaned version of the "host" column'),
          #   tags$li('"collection_date" column contains the date of collection in date format, be careful if the provider do not specify the month or day then the workflow automatically insert the "01" to the month or day, a new column is comming soon that makes clear if the provider submitted the date in day level'),
          #   br(),
          #   tags$b("Head of the table:"),
          #   column(
          #     width = 12,
          #     DT::dataTableOutput("meta_head"),
          #     style = "overflow-x: scroll;"
          #   )
          # ),
          # 
          # 
          # box(
          #   title = 'Table "vcf":',
          #   status = "primary",
          #   width = "12", solidHeader = TRUE,
          #   "Contains all the mutations that derives from annotated VCF files",
          #   br(),
          #   "The workflow that generates the VCF files is here: ",
          #   tags$a(href = "https://github.com/enasequence/covid-sequence-analysis-workflow/blob/master/workflow.nf", "link"),
          #   br(),
          #   tags$b("Head of the table:"),
          #   column(
          #     width = 12,
          #     
          #     DT::dataTableOutput("vcf_head"),
          #     style = "overflow-x: scroll;"
          #   )
          # ),
          # 
          # 
          # box(
          #   title = 'Table "cov":',
          #   status = "primary",
          #   width = "12", solidHeader = TRUE,
          #   "Contains the coverage in each position of the virus genome",
          #   br(),
          #   tags$b("Head of the table:"),
          #   column(
          #     width = 12,
          #     DT::dataTableOutput("cov_head")
          #   )
          # ),
          # 
          # box(
          #   title = 'Table "lineage_def":',
          #   status = "primary",
          #   width = "12", solidHeader = TRUE,
          #   "Contains the muation pattern of variants",
          #   br(),
          #   tags$b("Head of the table:"),
          #   column(
          #     width = 12,
          #     DT::dataTableOutput("lineage_def_head"),
          #     style = "overflow-x: scroll;"
          #   )
          # ),
          # 
          
          # box(
          #   title = 'Table "lineage":',
          #   status = "primary",
          #   width = "12", solidHeader = TRUE,
          #   "Contains the variant type of samples",
          #   br(),
          #   tags$b("Head of the table:"),
          #   column(
          #     width = 12,
          #     DT::dataTableOutput("lineage_head")
          #   )
          # ),
          br(), br(), br(), br(), br(),'You can send any comments/suggestions: Kriszián Papp (krisztian.papp@phys-gs.elte.hu)', br(),
        ),
      )
    ),
    
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
                               

                              
                              
                              ")))
  )
)

############################################################################################ 
# Server part of the the app
############################################################################################

server <- function(input, output) {
  output$eu_map <- renderHighchart({
    map <- jsonlite::fromJSON(txt = "eugeo.json", simplifyVector = FALSE)
    
    if (input$relative_to_population_eu) {
      # y <- tbl(con, "ecdc_covid_country_weekly") %>%
      #   left_join(tbl(con, "country"), by = c("country_id" = "id")) %>%
      #   select(country_name, population) %>%
      #   distinct() %>%
      #   rename(country = "country_name") %>%
      #   collect()
      # x <- app_country_samples %>%
      #   left_join(y) %>%
      #   mutate(
      #     n_sample = round(n_sample / population * 1000000, 3),
      #     log_n_sample = log10(n_sample)
      #   )
      # 
      
      highchart() %>%
        hc_add_series_map(
          map, app_country_samples,
          value = "relative_log_n_sample", joinBy = c("name", "country")
        ) %>%
        # hc_colorAxis(stops = color_stops()) %>%
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
        # hc_colorAxis(stops = color_stops()) %>%
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
      # y <- tbl(con, "ecdc_covid_country_weekly") %>%
      #   select(country_id, population) %>%
      #   distinct() %>%
      #   left_join(tbl(con, "country"), by = c("country_id" = "id")) %>%
      #   select(country_name, population) %>%
      #   rename(country = "country_name") %>%
      #   collect()
      # x <- app_country_samples %>%
      #   left_join(y) %>%
      #   mutate(
      #     n_sample = round(n_sample / population * 1000000, 3),
      #     log_n_sample = log10(n_sample)
      #   )
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
        # hc_colorAxis(stops = color_stops()) %>%
        hc_legend(labelFormat = "", title = list(text = "Number of samples in log10 scale")) %>%
        hc_title(text = "Number of raw SARS-CoV-2 sequence from worldwide") %>%
        hc_subtitle(text = "Move mouse above the country to see the numbers") %>%
        hc_mapNavigation(enabled = TRUE) %>%
        hc_tooltip(useHTML = TRUE, headerFormat = "", pointFormat = "{point.n_sample} samples derived from {point.country}")%>%
        hc_colors(colorstw)
    }
  })
  
  
  
  output$structure_open <- renderNGLVieweR({
    # x <- "(:A OR :B OR :C) AND (501 OR 570 OR 681 OR 716 OR 982 OR 1118)"
    selected_AA <- lineage_def_data %>%
      dplyr::filter(variant_id == !!input$selected_lineage) %>%
      dplyr::filter(
        # type == "SNP",
        gene == "S"
      ) %>%
      dplyr::select("protein_codon_position") %>%
      collect()
    y <- paste(as.character(selected_AA$protein_codon_position), "OR", collapse = " OR ")
    x <- paste("(:A) AND ( ", y, " )", sep = "")
    
    # x <- "(:A OR :B OR :C) AND (501 OR 570 OR 681 OR 716 OR 982 OR 1118)"
    
    
    NGLVieweR("7A95") %>%
      addRepresentation("cartoon", param = list(name = "S1", color = "brown", sele = ":A")) %>%
      addRepresentation("cartoon", param = list(name = "ACE2", color = "darkblue", sele = ":D")) %>%
      addRepresentation("cartoon", param = list(name = "S2", color = "lightgray", sele = ":B")) %>%
      addRepresentation("cartoon", param = list(name = "S3", color = "lightgray", sele = ":C")) %>%
      addRepresentation("label", param = list(
        sele = ":D AND 608",
        labelType = "format",
        labelFormat = "ACE2", # or enter custom text
        # labelFormat='[%(resname)s]%(resno)s', #or enter custom text
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
        # labelFormat='alma', #or enter custom text
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
        # backgroundColor="blue",
        # backgroundOpacity=0.5
      )) %>%
      addRepresentation("label", param = list(
        sele = ":A AND 1146",
        labelType = "format",
        labelFormat = "S (open state)", # or enter custom text
        # labelFormat='[%(resname)s]%(resno)s', #or enter custom text
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
    # setSpin()
  })
  
  
  
  
  
  
  
  
  output$structure_closed <- renderNGLVieweR({
    # x <- "(:A OR :B OR :C) AND (501 OR 570 OR 681 OR 716 OR 982 OR 1118)"
    selected_AA <- lineage_def_data %>%
      dplyr::filter(variant_id == !!input$selected_lineage) %>%
      dplyr::filter(
        # type == "SNP",
        gene == "S"
      ) %>%
      dplyr::select("protein_codon_position") %>%
      collect()
    y <- paste(as.character(selected_AA$protein_codon_position), "OR", collapse = " OR ")
    x <- paste("(:B) AND ( ", y, " )", sep = "")
    
    # x <- "(:A OR :B OR :C) AND (501 OR 570 OR 681 OR 716 OR 982 OR 1118)"
    
    
    NGLVieweR("7A95") %>%
      addRepresentation("cartoon", param = list(name = "S1", color = "lightgray", sele = ":A")) %>%
      addRepresentation("cartoon", param = list(name = "ACE2", color = "lightgray", sele = ":D")) %>%
      addRepresentation("cartoon", param = list(name = "S2", color = "orange", sele = ":B")) %>%
      addRepresentation("cartoon", param = list(name = "S3", color = "lightgray", sele = ":C")) %>%
      addRepresentation("label", param = list(
        sele = x,
        labelType = "format",
        # labelFormat='alma', #or enter custom text
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
        # backgroundColor="blue",
        # backgroundOpacity=0.5
      )) %>%
      addRepresentation("label", param = list(
        sele = ":B AND 1146",
        labelType = "format",
        labelFormat = "S (closed state)", # or enter custom text
        # labelFormat='[%(resname)s]%(resno)s', #or enter custom text
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
    # setSpin()
  })
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  output$euPlot <- renderHighchart({
    eu <- c(
      "United Kingdom", "Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus", "Czech Republic", "Denmark", "Estonia", "Finland", "France", "Germany", "Greece", "Hungary",
      "Ireland", "Italy", "Latvia", "Lithuania", "Luxembourg", "Malta", "Netherlands", "Poland", "Portugal", "Romania", "Slovakia", "Slovenia", "Spain", "Sweden"
    )
    
    # x <- tbl(con, "app_human_meta_mv") %>%
    #   collect()
    # x <- tbl(con, "metadata") %>%
    #   left_join(tbl(con, "country"), by = c("country_id" = "id")) %>%
    #   dplyr::mutate(country = ifelse(country_name == "USA", "United States", country_name)) %>%
    #   filter(!is.na(collection_date)) %>%
    #   dplyr::filter(collection_date < today()) %>%
    #   dplyr::filter(collection_date > as.Date("2019-12-01")) %>%
    #   filter(!is.na(country_name)) %>%
    #   dplyr::filter(host_id == 3) %>%
    #   # dplyr::filter(clean_collection_date>as.Date("2020-03-15"))%>%
    #   dplyr::rename(
    #     country = "country_name",
    #     date_year = "date_isoyear",
    #     date_week = "date_isoweek"
    #   ) %>%
    #   dplyr::select(ena_run, Country, clean_collection_date, date_year, date_week) %>%
    #   group_by(country, date_year, date_week) %>%
    #   dplyr::summarise(weekly_sample = n()) %>%
    #   collect()
    x <- app_human_meta_mv %>%
      # mutate(date_week_iso = ifelse(date_week < 10, str_c("0", as.character(date_week)), as.character(date_week))) %>%
      # mutate(date = ISOweek2date(paste(date_year, "-W", date_week_iso, "-1", sep = ""))) %>%
      dplyr::filter(country_name %in% local(eu)) %>%
      group_by(country_name) %>%
      arrange(date) %>%
      mutate(sum_weekly_sample = cumsum(weekly_sample)) %>%
      ungroup()
    
    
    
    if (input$eu_graph_type=="Weekly sample number"){
      highchart(type = "stock") %>%
        # hc_add_series(x, "scatter", hcaes(x=clean_collection_date, y=n, group=Country),
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
          # hc_add_series(x, "scatter", hcaes(x=clean_collection_date, y=n, group=Country),
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
        
        # x <- tbl(con, "app_human_meta_mv_jhd") %>%
        #   collect()
        
        # x <- tbl(con, "meta") %>%
        #   dplyr::filter(clean_host == "Homo sapiens") %>%
        #   dplyr::mutate(country = ifelse(country == "USA", "United States", country)) %>%
        #   dplyr::filter(!is.na(clean_collection_date)) %>%
        #   dplyr::filter(clean_collection_date > as.Date("2020-03-15")) %>%
        #   dplyr::filter(clean_collection_date < today()) %>%
        #   dplyr::rename(
        #     country_name = "country",
        #     date_year = "date_isoyear",
        #     date_week = "date_isoweek"
        #   ) %>%
        #   dplyr::select(ena_run, country_name, clean_collection_date, date_year, date_week) %>%
        #   group_by(country_name, date_year, date_week) %>%
        #   dplyr::summarise(weekly_sample = n()) %>%
        #   # left_join(tbl(con2, "country_iso"), copy = TRUE)%>%
        #   left_join(tbl(con, "ecdc_covid_country_weekly")) %>%
        #   filter(cases != 0) %>%
        #   mutate(pct = round(weekly_sample / cases * 100, 2)) %>%
        #   dplyr::select(country_name, iso_a3, date_year, date_week, weekly_sample, cases, pct) %>%
        #   collect()
        
        x <- app_human_meta_mv_jhd %>%
          # mutate(date_week_iso = ifelse(date_week < 10, str_c("0", as.character(date_week)), as.character(date_week))) %>%
          # mutate(date = ISOweek2date(paste(date_year, "-W", date_week_iso, "-1", sep = ""))) %>%
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
      # mutate(date_week_iso = ifelse(date_week < 10, str_c("0", as.character(date_week)), as.character(date_week))) %>%
      # mutate(date = ISOweek2date(paste(date_year, "-W", date_week_iso, "-1", sep = ""))) %>%
      dplyr::filter(country_name %in% input$world_select_country) %>%
      group_by(country_name) %>%
      arrange(date) %>%
      mutate(sum_weekly_sample = cumsum(weekly_sample)) %>%
      ungroup()
    
    
    if (input$world_graph_type=="Weekly sample number"){
      highchart(type = "stock") %>%
        # hc_add_series(x, "scatter", hcaes(x=clean_collection_date, y=n, group=Country),
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
          # hc_add_series(x, "scatter", hcaes(x=clean_collection_date, y=n, group=Country),
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
        
        # x <- tbl(con, "app_human_meta_mv_jhd") %>%
        #   collect()
        # 
        # x <- tbl(con, "meta") %>%
        #   dplyr::filter(clean_host == "Homo sapiens") %>%
        #   dplyr::mutate(country = ifelse(country == "USA", "United States", country)) %>%
        #   dplyr::filter(!is.na(clean_collection_date)) %>%
        #   dplyr::filter(clean_collection_date > as.Date("2020-03-15")) %>%
        #   dplyr::filter(country %in% local(input$world_select_country))%>%
        #   dplyr::rename(
        #     country_name = "country",
        #     date_year = "date_isoyear",
        #     date_week = "date_isoweek"
        #   ) %>%
        #   dplyr::select(ena_run, country_name, clean_collection_date, date_year, date_week) %>%
        #   group_by(country_name, date_year, date_week) %>%
        #   dplyr::summarise(weekly_sample = n()) %>%
        #   # left_join(tbl(con2, "country_iso"), copy = TRUE)%>%
        #   left_join(tbl(con, "ecdc_covid_country_weekly")) %>%
        #   filter(cases != 0) %>%
        #   mutate(pct = round(weekly_sample / cases * 100, 2)) %>%
        #   dplyr::select(country_name, iso_a3, date_year, date_week, weekly_sample, cases, pct) %>%
        #   collect()
        # 
        x <- app_human_meta_mv_jhd %>%
          # mutate(date_week_iso = ifelse(date_week < 10, str_c("0", as.character(date_week)), as.character(date_week))) %>%
          # mutate(date = ISOweek2date(paste(date_year, "-W", date_week_iso, "-1", sep = "")))%>%
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
  
  
  
  
  
  # output$vcf_head <- DT::renderDataTable({
  #   DT::datatable(
  #     {
  #       tbl(con, "vcf") %>%
  #         head() %>%
  #         collect()
  #     },
  #     # selection = 'single',
  #     # extensions = 'Buttons',
  #     options = list(
  #       lengthChange = FALSE,
  #       paging = FALSE,
  #       dom = "t"
  #     )
  #   )
  # })
  
  # output$cov_head <- DT::renderDataTable({
  #   DT::datatable(
  #     {
  #       tbl(con, "cov") %>%
  #         head() %>%
  #         collect()
  #     },
  #     # selection = 'single',
  #     # extensions = 'Buttons',
  #     options = list(
  #       lengthChange = FALSE,
  #       paging = FALSE,
  #       dom = "t"
  #     )
  #   )
  # })
  
  
  # output$lineage_def_head <- DT::renderDataTable({
  #   DT::datatable(
  #     {
  #       tbl(con, "lineage_def") %>%
  #         head() %>%
  #         collect()
  #     },
  #     # selection = 'single',
  #     # extensions = 'Buttons',
  #     options = list(
  #       lengthChange = FALSE,
  #       paging = FALSE,
  #       dom = "t"
  #     )
  #   )
  # })
  
  # output$lineage_head <- DT::renderDataTable({
  #   DT::datatable(
  #     {
  #       tbl(con, "lineage") %>%
  #         head() %>%
  #         collect()
  #     },
  #     # selection = 'single',
  #     # extensions = 'Buttons',
  #     options = list(
  #       lengthChange = FALSE,
  #       paging = FALSE,
  #       dom = "t"
  #     )
  #   )
  # })
  
  # output$meta_head <- DT::renderDataTable({
  #   DT::datatable(
  #     {
  #       tbl(con, "metadata") %>%
  #         head(2) %>%
  #         collect()
  #     },
  #     # selection = 'single',
  #     # extensions = 'Buttons',
  #     options = list(
  #       lengthChange = FALSE,
  #       paging = FALSE,
  #       dom = "t"
  #     )
  #   )
  # })
  # 
  
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
      # dplyr::filter(country_name == "United Kingdom")%>%
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
  
  # output$jstree <-
  #   renderJstree(
  #     jstree(nodes, search = FALSE, theme= "proton", multiple = FALSE, checkboxes = FALSE)
  #   )
  # 
  # filtered_column_description <- reactive({
  #   if (!length(input$jstree_selected)) {
  #     selected_table <- "Nothing selected"
  #   }  else {
  #     selected_table <-input$jstree_selected[[1]]$text 
  #   }
  #   selected_table
  # })
  
  # 
  # output$filtered_table_name<- renderText({
  #   x <- as.character(filtered_column_description())
  #   if (x%in%c("Nothing selected", "tables", "views", "functions")) {
  #     x_out <- "Select a single table/view/function on the left (double click on tables/views to unfold the leaves)"
  #   }
  #   else {
  #     x_out <- paste0("Name: ", x)
  #   }
  #   x_out
  # })
  # 
  # 
  # output$filtered_table_title<- renderText({
  #   x <- as.character(filtered_column_description())
  #   if (x%in%c("Nothing selected", "tables", "views", "functions")) {
  #     x_out <- " "
  #   }
  #   else {
  #     x_out <- as.character(table_description[table_description$table_name==x, 'title'])
  #   }
  #   x_out
  # })  
  # 
  # output$filtered_table_description<- renderText({
  #   x <- as.character(filtered_column_description())
  #   if (x%in%c("Nothing selected", "tables", "views", "functions")) {
  #     x_out <- " "
  #   }
  #   else {
  #     x_out <- as.character(table_description[table_description$table_name==x, 'description'])
  #   }
  #   x_out
  # })
  # 
  
  # output$filtered_table_sql<- renderText({
  #   
  #   x <- as.character(filtered_column_description())
  #   if (x%in%c("Nothing selected", "tables", "views", "functions")) {
  #     x_out <- " "
  #   }
  #   else {
  #     table_type <- tbl(con, "table_description") %>%
  #       dplyr::filter(table_name==x)%>%
  #       dplyr::select(type)%>%
  #       collect()
  #     
  #     if (as.character(table_type)=="tables") {
  #       x_out <- " "
  #     }
  #     
  #     if (as.character(table_type)=="functions"){
  #       sql_query <- SQL(paste0("SELECT prosrc FROM pg_catalog.pg_proc WHERE proname=\'", x, "\';"))
  #       x_out <- as.character(dbGetQuery(con, sql_query)) 
  #     }
  #     
  #     if (as.character(table_type)=="views") {
  #       sql_query <- SQL(paste0("SELECT definition FROM pg_matviews WHERE matviewname=\'", x, "\';"))
  #       x_out <- as.character(dbGetQuery(con, sql_query)) 
  #     }
  #     
  #   }
  #   x_out
  #   
  # })
  # 
  # output$table_selected_column_description <- DT::renderDataTable(
  #   DT::datatable(
  #     {
  #       x <- as.character(filtered_column_description())
  #       
  #       if (x%in%c("Nothing selected", "tables", "views", "functions")) {
  #         table_out_column_description <- tibble()}
  #       else{
  #         
  #         table_type <- tbl(con, "table_description") %>%
  #           dplyr::filter(table_name==x)%>%
  #           dplyr::select(type)%>%
  #           collect()
  #         if (as.character(table_type)=="functions") {
  #           table_out_column_description <- tibble()}          
  #         else{
  #           if (as.character(table_type)=="tables"){
  #             table_out_column_description <- tbl(con, "column_description") %>%
  #               dplyr::filter(table_name==x)%>%
  #               collect() %>%
  #               inner_join(dbGetQuery(con, "SELECT table_name, data_type, column_name FROM information_schema.columns WHERE table_schema='public';"))%>%
  #               dplyr::select(column_name, data_type, description)%>%
  #               rename("Column name"=column_name,
  #                      "Type"=data_type,
  #                      "Description"=description)
  #             table_out_column_description}
  #           
  #           else {
  #             url <- paste0("SELECT a.attname,
  #               pg_catalog.format_type(a.atttypid, a.atttypmod),
  #               a.attnotnull
  #               FROM pg_attribute a
  #               JOIN pg_class t on a.attrelid = t.oid
  #               JOIN pg_namespace s on t.relnamespace = s.oid
  #               WHERE a.attnum > 0 
  #               AND NOT a.attisdropped
  #               AND t.relname = \'", x, "\' --<< replace with the name of the MV 
  #               AND s.nspname = 'public' --<< change to the schema your MV is in 
  #               ORDER BY a.attnum;")
  #             table_out_column_description <- tbl(con, "column_description") %>%
  #               dplyr::filter(table_name==x)%>%
  #               collect() %>%
  #               inner_join(dbGetQuery(con, url), by=c("column_name"="attname"))%>%
  #               dplyr::select(column_name, format_type, description)%>%
  #               rename("Column name"=column_name,
  #                      "Type"=format_type,
  #                      "Description"=description)
  #             table_out_column_description
  #           }
  #           
  #           
  #         }
  #         
  #         
  #         
  #       }
  #       
  #     },
  #     
  #     # selection = 'single',
  #     # extensions = 'Buttons',
  #     # filter = 'top',
  #     options = list(
  #       lengthChange = FALSE,
  #       paging = FALSE,
  #       dom = "t"
  #     )
  #   ),
  # )
  # 
  
  
  
  output$table <- DT::renderDataTable(
    DT::datatable(
      {
        lineage_def_data %>%
          distinct (variant_id, .keep_all = TRUE) %>%
          select (variant_id, pango, description) %>%
          dplyr::filter(variant_id == input$selected_lineage)
      },
      
      # selection = 'single',
      # extensions = 'Buttons',
      # filter = 'top',
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
