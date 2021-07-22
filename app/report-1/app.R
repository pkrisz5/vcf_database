library(shiny)
library(DBI)
library(shinydashboard)
library(tidyverse)
library(lubridate)
library(plotly)
library(highcharter)
library(pool)
library(shinyBS)
library(config)
library(ISOweek)
library(NGLVieweR)
# library(shinyWidgets)
#  devtools::install_github("gadenbuie/shinyThings")
library(shinyThings)


app_version <- "v_001.000"

config <- config::get()

con <- dbPool(
  drv = RPostgreSQL::PostgreSQL(),
  dbname = config$dbname,
  host = config$host,
  port = config$port,
  user = config$user,
  password = config$password
)

onStop(function() {
  poolClose(con)
})


country_samples <- tbl(con, "meta") %>%
  filter(clean_host == "Homo sapiens") %>%
  filter(!is.na(clean_collection_date)) %>%
  group_by(clean_country) %>%
  summarise(n_sample = n()) %>%
  collect()
country_samples <- country_samples %>%
  drop_na() %>%
  mutate(log_n_sample = log10(n_sample)) %>%
  as.data.frame()



lineage <- tbl(con, "meta") %>%
  dplyr::mutate(clean_country = ifelse(clean_country == "USA", "United States", clean_country)) %>%
  filter(!is.na(clean_collection_date)) %>%
  filter(clean_host == "Homo sapiens") %>%
  select(ena_run:clean_country, clean_collection_date) %>%
  inner_join(tbl(con, "lineage")) %>%
  select(ena_run:variant_id) %>%
  dplyr::filter(clean_collection_date > as_date("2020-01-01")) %>%
  group_by(clean_collection_date, clean_country, variant_id) %>%
  dplyr::summarise(n = n()) %>%
  collect() %>%
  drop_na() %>%
  dplyr::rename(Country = "clean_country")

lineage0 <- tbl(con, "meta") %>%
  dplyr::mutate(clean_country = ifelse(clean_country == "USA", "United States", clean_country)) %>%
  filter(!is.na(clean_collection_date)) %>%
  filter(clean_host == "Homo sapiens") %>%
  select(ena_run:clean_country, clean_collection_date) %>%
  dplyr::filter(clean_collection_date > as_date("2020-01-01")) %>%
  group_by(clean_collection_date, clean_country) %>%
  dplyr::summarise(n = n()) %>%
  collect() %>%
  drop_na() %>%
  dplyr::rename(
    Country = "clean_country",
    n_all = "n"
  )

lineage2 <- lineage %>%
  inner_join(lineage0, by = c("clean_collection_date", "Country")) %>%
  mutate(pct = n / n_all * 100)

lineage_def <- tbl(con, "lineage_def") %>%
  select(variant_id:nextstrain, description) %>%
  collect() %>%
  distinct(variant_id, .keep_all = TRUE)



### This part will create the master table for variant graphs

new_cases <- tbl(con, "meta") %>%
  dplyr::mutate(clean_country = ifelse(clean_country == "USA", "United States", clean_country)) %>%
  filter(!is.na(clean_collection_date)) %>%
  dplyr::filter(clean_host == "Homo sapiens") %>%
  dplyr::filter(clean_collection_date > as.Date("2020-03-15")) %>%
  dplyr::rename(
    country_name = "clean_country",
    date_year = "date_isoyear",
    date_week = "date_isoweek"
  ) %>%
  dplyr::select(ena_run, country_name, clean_collection_date, date_year, date_week) %>%
  group_by(country_name, date_year, date_week) %>%
  dplyr::summarise(weekly_sample = n()) %>%
  left_join(tbl(con, "ecdc_covid_country_weekly")) %>%
  collect()
new_cases <- new_cases %>%
  mutate(date_week_iso = ifelse(date_week < 10, str_c("0", as.character(date_week)), as.character(date_week))) %>%
  mutate(date = ISOweek2date(paste(date_year, "-W", date_week_iso, "-1", sep = ""))) %>%
  dplyr::select(country_name, date, date_year, date_week, weekly_sample, ecdc_covid_country_weekly_cases)

variants_weekly <- tbl(con, "meta") %>%
  dplyr::mutate(clean_country = ifelse(clean_country == "USA", "United States", clean_country)) %>%
  filter(!is.na(clean_collection_date)) %>%
  dplyr::filter(clean_host == "Homo sapiens") %>%
  dplyr::filter(clean_collection_date > as.Date("2020-03-15")) %>%
  dplyr::rename(
    country_name = "clean_country",
    date_year = "date_isoyear",
    date_week = "date_isoweek"
  ) %>%
  dplyr::select(ena_run, country_name, clean_collection_date, date_year, date_week) %>%
  inner_join(tbl(con, "lineage")) %>%
  group_by(country_name, date_year, date_week, variant_id) %>%
  dplyr::summarise(weekly_variant_sample = n()) %>%
  collect()

variants_weekly <- variants_weekly %>%
  pivot_wider(names_from = variant_id, values_from = weekly_variant_sample)

variants_weekly$cases_with_variant_id <- rowSums(variants_weekly[, c(-1, -2, -3)], na.rm = TRUE)


variant_master_table <- new_cases %>%
  left_join(variants_weekly) %>%
  mutate(
    `Other variants` = weekly_sample - cases_with_variant_id,
    `Non-sequenced new cases` = ecdc_covid_country_weekly_cases - weekly_sample
  ) %>%
  dplyr::select(-weekly_sample, -ecdc_covid_country_weekly_cases, -cases_with_variant_id) %>%
  pivot_longer(cols = !any_of(c("country_name", "date", "date_year", "date_week")))

# This part creates a table for world data plot

worldplot_data <- tbl(con, "meta") %>%
  dplyr::mutate(clean_country = ifelse(clean_country == "USA", "United States", clean_country)) %>%
  filter(!is.na(clean_collection_date)) %>%
  filter(!is.na(clean_country)) %>%
  dplyr::filter(clean_host == "Homo sapiens") %>%
  dplyr::filter(clean_collection_date>as.Date("2020-03-15"))%>%
  dplyr::rename(
    Country = "clean_country",
    date_year = "date_isoyear",
    date_week = "date_isoweek"
  ) %>%
  dplyr::select(ena_run, Country, clean_collection_date, date_year, date_week) %>%
  group_by(Country, date_year, date_week) %>%
  dplyr::summarise(weekly_sample = n()) %>%
  collect()
worldplot_data <- worldplot_data %>%
  mutate(date_week_iso = ifelse(date_week < 10, str_c("0", as.character(date_week)), as.character(date_week))) %>%
  mutate(date = ISOweek2date(paste(date_year, "-W", date_week_iso, "-1", sep = ""))) %>%
  group_by(Country) %>%
  arrange(date) %>%
  mutate(sum_weekly_sample = cumsum(weekly_sample)) %>%
  ungroup()


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
    tabItems(
      tabItem(
        tabName = "country_map",
        tabsetPanel(
          type = "tabs",
          
          
          tabPanel(
            "EU",
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
      
      
      
      tabItem(
        tabName = "variants",
        fluidRow(
          column(
            width = 4,
            selectInput("selected_country_for_variants",
                        label = "Select country",
                        choices = unique(variant_master_table$country_name),
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
      
      
      
      
      tabItem(
        tabName = "country_graph",
        tabsetPanel(
          type = "tabs",
          
          tabPanel(
            "EU",
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
                                choices = unique(worldplot_data$Country),
                                selected = c("Spain", "Australia", "United States"),
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
      
      
      
      tabItem(
        tabName = "lineage_graph_country",
        fluidRow(),
        radioButtons("selected_country",
                     label = "Select country",
                     choices = unique(lineage$Country),
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
      
      
      
      tabItem(
        tabName = "lineage_graph_lineage",
        fluidRow(
          radioButtons("selected_lineage",
                       label = "Select VOC/VOI",
                       choices = unique(lineage$variant_id),
                       inline = TRUE, selected = unique(lineage$variant_id)[1]
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
            footer = HTML('<p> Click on the 3D structures <a href="https://www.rcsb.org/structure/7A95"> (PDB: 7A95)</a>, and move your mouse to spin or use mouse wheel to zoom the protein for better view of the mutationsaaaa</p>'),
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
      
      tabItem(
        tabName = "menu_info",
        fluidRow(
          tags$h3("Overview"), br(),
          # tags$img(src='vcf_database_overview.png'),
          
          infoBoxOutput("version"), br(), br(), br(), br(), br(),
          tags$h4("The database contains three tables:"), br(),
          infoBoxOutput("vcf_count"),
          infoBoxOutput("cov_count"),
          infoBoxOutput("meta_count"),
          box(
            title = 'Table "meta":',
            status = "primary",
            width = "12", solidHeader = TRUE,
            "Contains the metadata for each ENA run accession number", br(), "A few extra column was inserted",
            tags$li('"clean_counry" column contains only country names (the original "country" column sometimes also contains smaller region names, not only country name)'),
            tags$li('"clean_host" column contains "Homo sapiens" if sample derived from human host (this is a cleaned version of the "host" column'),
            tags$li('"collection_date" column contains the date of collection in date format, be careful if the provider do not specify the month or day then the workflow automatically insert the "01" to the month or day, a new column is comming soon that makes clear if the provider submitted the date in day level'),
            br(),
            tags$b("Head of the table:"),
            column(
              width = 12,
              DT::dataTableOutput("meta_head"),
              style = "overflow-x: scroll;"
            )
          ),
          
          
          box(
            title = 'Table "vcf":',
            status = "primary",
            width = "12", solidHeader = TRUE,
            "Contains all the mutations that derives from annotated VCF files",
            br(),
            "The workflow that generates the VCF files is here: ",
            tags$a(href = "https://github.com/enasequence/covid-sequence-analysis-workflow/blob/master/workflow.nf", "link"),
            br(),
            tags$b("Head of the table:"),
            column(
              width = 12,
              
              DT::dataTableOutput("vcf_head"),
              style = "overflow-x: scroll;"
            )
          ),
          
          
          box(
            title = 'Table "cov":',
            status = "primary",
            width = "12", solidHeader = TRUE,
            "Contains the coverage in each position of the virus genome",
            br(),
            tags$b("Head of the table:"),
            column(
              width = 12,
              DT::dataTableOutput("cov_head")
            )
          ),
          
          box(
            title = 'Table "lineage_def":',
            status = "primary",
            width = "12", solidHeader = TRUE,
            "Contains the muation pattern of variants",
            br(),
            tags$b("Head of the table:"),
            column(
              width = 12,
              DT::dataTableOutput("lineage_def_head"),
              style = "overflow-x: scroll;"
            )
          ),
          
          
          box(
            title = 'Table "lineage":',
            status = "primary",
            width = "12", solidHeader = TRUE,
            "Contains the variant type of samples",
            br(),
            tags$b("Head of the table:"),
            column(
              width = 12,
              DT::dataTableOutput("lineage_head")
            )
          ),
          'You can send any comments/suggestions: Kriszián Papp (krisztian.papp@phys-gs.elte.hu)', br(),
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
         .skin-blue .content{
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
      y <- tbl(con, "ecdc_covid_country_weekly") %>%
        select(country_name, population) %>%
        distinct() %>%
        rename(clean_country = "country_name") %>%
        collect()
      x <- country_samples %>%
        left_join(y) %>%
        mutate(
          clean_country = ifelse(clean_country == "USA", "United States of America", clean_country),
          n_sample = round(n_sample / population * 1000000, 3),
          log_n_sample = log10(n_sample)
        )
      
      
      highchart() %>%
        hc_add_series_map(
          map, x,
          value = "log_n_sample", joinBy = c("name", "clean_country")
        ) %>%
        # hc_colorAxis(stops = color_stops()) %>%
        hc_legend(labelFormat = "", title = list(text = "Number of samples relative to 1 million citizen in log10 scale")) %>%
        hc_title(text = "Number of raw SARS-CoV-2 sequence from EU") %>%
        hc_subtitle(text = "Move mouse above a country to see the numbers") %>%
        hc_mapNavigation(enabled = TRUE) %>%
        hc_tooltip(useHTML = TRUE, headerFormat = "", pointFormat = "{point.n_sample} samples relative to 1 million derived from  {point.clean_country}")
    }
    else {
      x <- country_samples %>%
        mutate(clean_country = ifelse(clean_country == "USA", "United States of America", clean_country))
      highchart() %>%
        hc_add_series_map(
          map, x,
          value = "log_n_sample", joinBy = c("name", "clean_country")
        ) %>%
        # hc_colorAxis(stops = color_stops()) %>%
        hc_legend(labelFormat = "", title = list(text = "Number of samples in log10 scale")) %>%
        hc_title(text = "Number of raw SARS-CoV-2 sequence from EU") %>%
        hc_subtitle(text = "Move mouse above a country to see the numbers") %>%
        hc_mapNavigation(enabled = TRUE) %>%
        hc_tooltip(useHTML = TRUE, headerFormat = "", pointFormat = "{point.n_sample} samples derived from {point.clean_country}")
    }
  })
  
  
  
  output$world_map <- renderHighchart({
    if (input$relative_to_population_world) {
      y <- tbl(con, "ecdc_covid_country_weekly") %>%
        select(country_name, population) %>%
        distinct() %>%
        rename(clean_country = "country_name") %>%
        mutate(
          clean_country = ifelse(clean_country == "United States", "USA", clean_country),
          clean_country = ifelse(clean_country == "Russian Federation", "Russia", clean_country)
        ) %>%
        collect()
      x <- country_samples %>%
        left_join(y) %>%
        mutate(
          clean_country = ifelse(clean_country == "USA", "United States of America", clean_country),
          n_sample = round(n_sample / population * 1000000, 3),
          log_n_sample = log10(n_sample)
        )
      highchart() %>%
        hc_add_series_map(
          worldgeojson, x,
          value = "log_n_sample", joinBy = c("name", "clean_country")
        ) %>%
        # hc_colorAxis(stops = color_stops()) %>%
        hc_legend(labelFormat = "", title = list(text = "Number of samples relative to 1 million citizen in log10 scale")) %>%
        hc_title(text = "Number of raw SARS-CoV-2 sequence from worldwide") %>%
        hc_subtitle(text = "Move mouse above the country to see the numbers") %>%
        hc_mapNavigation(enabled = TRUE) %>%
        hc_tooltip(useHTML = TRUE, headerFormat = "", pointFormat = "{point.n_sample} samples relative to 1 million derived from {point.clean_country}")
    }
    else {
      x <- country_samples %>%
        mutate(clean_country = ifelse(clean_country == "USA", "United States of America", clean_country))
      highchart() %>%
        hc_add_series_map(
          worldgeojson, x,
          value = "log_n_sample", joinBy = c("name", "clean_country")
        ) %>%
        # hc_colorAxis(stops = color_stops()) %>%
        hc_legend(labelFormat = "", title = list(text = "Number of samples in log10 scale")) %>%
        hc_title(text = "Number of raw SARS-CoV-2 sequence from worldwide") %>%
        hc_subtitle(text = "Move mouse above the country to see the numbers") %>%
        hc_mapNavigation(enabled = TRUE) %>%
        hc_tooltip(useHTML = TRUE, headerFormat = "", pointFormat = "{point.n_sample} samples derived from {point.clean_country}")
    }
  })
  
  
  
  output$structure_open <- renderNGLVieweR({
    # x <- "(:A OR :B OR :C) AND (501 OR 570 OR 681 OR 716 OR 982 OR 1118)"
    selected_AA <- tbl(con, "lineage_def") %>%
      dplyr::filter(variant_id == !!input$selected_lineage) %>%
      dplyr::filter(
        type == "SNP",
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
    selected_AA <- tbl(con, "lineage_def") %>%
      dplyr::filter(variant_id == !!input$selected_lineage) %>%
      dplyr::filter(
        type == "SNP",
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
    
    
    x <- tbl(con, "meta") %>%
      dplyr::mutate(clean_country = ifelse(clean_country == "USA", "United States", clean_country)) %>%
      filter(!is.na(clean_collection_date)) %>%
      filter(!is.na(clean_country)) %>%
      dplyr::filter(clean_host == "Homo sapiens") %>%
      # dplyr::filter(clean_collection_date>as.Date("2020-03-15"))%>%
      dplyr::rename(
        Country = "clean_country",
        date_year = "date_isoyear",
        date_week = "date_isoweek"
      ) %>%
      dplyr::select(ena_run, Country, clean_collection_date, date_year, date_week) %>%
      group_by(Country, date_year, date_week) %>%
      dplyr::summarise(weekly_sample = n()) %>%
      collect()
    x <- x %>%
      mutate(date_week_iso = ifelse(date_week < 10, str_c("0", as.character(date_week)), as.character(date_week))) %>%
      mutate(date = ISOweek2date(paste(date_year, "-W", date_week_iso, "-1", sep = ""))) %>%
      dplyr::filter(Country %in% local(eu)) %>%
      group_by(Country) %>%
      arrange(date) %>%
      mutate(sum_weekly_sample = cumsum(weekly_sample)) %>%
      ungroup()
    
    
    
    if (input$eu_graph_type=="Weekly sample number"){
      highchart(type = "stock") %>%
        # hc_add_series(x, "scatter", hcaes(x=clean_collection_date, y=n, group=Country),
        hc_add_series(x, "scatter", hcaes(x = date, y = weekly_sample, group = Country),
                      tooltip = list(pointFormat = "Number of samples sequenced on a given week in {point.Country}:{point.weekly_sample}: ")
        ) %>%
        hc_title(text = "Number of samples derived from EU states on a given week") %>%
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
        hc_plotOptions(scatter = list(lineWidth = 1))
    } else {
      if (input$eu_graph_type=="Cumulative sample number"){
        highchart(type = "stock") %>%
          # hc_add_series(x, "scatter", hcaes(x=clean_collection_date, y=n, group=Country),
          hc_add_series(x, "scatter", hcaes(x = date, y = sum_weekly_sample, group = Country),
                        tooltip = list(pointFormat = "Number of samples sequenced in {point.Country}:{point.sum_weekly_sample}: ")
          ) %>%
          hc_title(text = "Number of samples derived from EU states on a given week") %>%
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
          hc_plotOptions(scatter = list(lineWidth = 1))
      } else {
        x <- tbl(con, "meta") %>%
          dplyr::filter(clean_host == "Homo sapiens") %>%
          dplyr::mutate(clean_country = ifelse(clean_country == "USA", "United States", clean_country)) %>%
          dplyr::filter(!is.na(clean_collection_date)) %>%
          dplyr::filter(clean_collection_date > as.Date("2020-03-15")) %>%
          dplyr::rename(
            country_name = "clean_country",
            date_year = "date_isoyear",
            date_week = "date_isoweek"
          ) %>%
          dplyr::select(ena_run, country_name, clean_collection_date, date_year, date_week) %>%
          group_by(country_name, date_year, date_week) %>%
          dplyr::summarise(weekly_sample = n()) %>%
          # left_join(tbl(con2, "country_iso"), copy = TRUE)%>%
          left_join(tbl(con, "ecdc_covid_country_weekly")) %>%
          filter(ecdc_covid_country_weekly_cases != 0) %>%
          mutate(pct = round(weekly_sample / ecdc_covid_country_weekly_cases * 100, 2)) %>%
          dplyr::select(country_name, iso_a3, date_year, date_week, weekly_sample, ecdc_covid_country_weekly_cases, pct) %>%
          collect()
        
        x <- x %>%
          mutate(date_week_iso = ifelse(date_week < 10, str_c("0", as.character(date_week)), as.character(date_week))) %>%
          mutate(date = ISOweek2date(paste(date_year, "-W", date_week_iso, "-1", sep = ""))) %>%
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
          hc_scrollbar(enabled = FALSE)
        
      }
      
    }
    
    
  })
  
  
  output$worldPlot <- renderHighchart({
    
    
    
    x <- worldplot_data %>%
      dplyr::filter(Country %in% input$world_select_country)
    
    if (input$world_graph_type=="Weekly sample number"){
      highchart(type = "stock") %>%
        # hc_add_series(x, "scatter", hcaes(x=clean_collection_date, y=n, group=Country),
        hc_add_series(x, "scatter", hcaes(x = date, y = weekly_sample, group = Country),
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
        hc_plotOptions(scatter = list(lineWidth = 1))
    } else {
      if (input$world_graph_type=="Cumulative sample number"){
        highchart(type = "stock") %>%
          # hc_add_series(x, "scatter", hcaes(x=clean_collection_date, y=n, group=Country),
          hc_add_series(x, "scatter", hcaes(x = date, y = sum_weekly_sample, group = Country),
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
          hc_plotOptions(scatter = list(lineWidth = 1))
      } else {
        x <- tbl(con, "meta") %>%
          dplyr::filter(clean_host == "Homo sapiens") %>%
          dplyr::mutate(clean_country = ifelse(clean_country == "USA", "United States", clean_country)) %>%
          dplyr::filter(!is.na(clean_collection_date)) %>%
          dplyr::filter(clean_collection_date > as.Date("2020-03-15")) %>%
          dplyr::filter(clean_country %in% local(input$world_select_country))%>%
          dplyr::rename(
            country_name = "clean_country",
            date_year = "date_isoyear",
            date_week = "date_isoweek"
          ) %>%
          dplyr::select(ena_run, country_name, clean_collection_date, date_year, date_week) %>%
          group_by(country_name, date_year, date_week) %>%
          dplyr::summarise(weekly_sample = n()) %>%
          # left_join(tbl(con2, "country_iso"), copy = TRUE)%>%
          left_join(tbl(con, "ecdc_covid_country_weekly")) %>%
          filter(ecdc_covid_country_weekly_cases != 0) %>%
          mutate(pct = round(weekly_sample / ecdc_covid_country_weekly_cases * 100, 2)) %>%
          dplyr::select(country_name, iso_a3, date_year, date_week, weekly_sample, ecdc_covid_country_weekly_cases, pct) %>%
          collect()
        
        x <- x %>%
          mutate(date_week_iso = ifelse(date_week < 10, str_c("0", as.character(date_week)), as.character(date_week))) %>%
          mutate(date = ISOweek2date(paste(date_year, "-W", date_week_iso, "-1", sep = "")))%>%
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
            type = input$world_log_lin
            #min = 0
          ) %>%
          hc_tooltip(crosshairs = TRUE) %>%
          hc_navigator(enabled = FALSE) %>%
          hc_scrollbar(enabled = FALSE)
        
      }
      
    }
    
    
  })
  
  
  
  
  
  output$vcf_head <- DT::renderDataTable({
    DT::datatable(
      {
        tbl(con, "vcf") %>%
          head() %>%
          collect()
      },
      # selection = 'single',
      # extensions = 'Buttons',
      options = list(
        lengthChange = FALSE,
        paging = FALSE,
        dom = "t"
      )
    )
  })
  
  output$cov_head <- DT::renderDataTable({
    DT::datatable(
      {
        tbl(con, "cov") %>%
          head() %>%
          collect()
      },
      # selection = 'single',
      # extensions = 'Buttons',
      options = list(
        lengthChange = FALSE,
        paging = FALSE,
        dom = "t"
      )
    )
  })
  
  
  output$lineage_def_head <- DT::renderDataTable({
    DT::datatable(
      {
        tbl(con, "lineage_def") %>%
          head() %>%
          collect()
      },
      # selection = 'single',
      # extensions = 'Buttons',
      options = list(
        lengthChange = FALSE,
        paging = FALSE,
        dom = "t"
      )
    )
  })
  
  output$lineage_head <- DT::renderDataTable({
    DT::datatable(
      {
        tbl(con, "lineage") %>%
          head() %>%
          collect()
      },
      # selection = 'single',
      # extensions = 'Buttons',
      options = list(
        lengthChange = FALSE,
        paging = FALSE,
        dom = "t"
      )
    )
  })
  
  output$meta_head <- DT::renderDataTable({
    DT::datatable(
      {
        tbl(con, "meta") %>%
          head(2) %>%
          collect()
      },
      # selection = 'single',
      # extensions = 'Buttons',
      options = list(
        lengthChange = FALSE,
        paging = FALSE,
        dom = "t"
      )
    )
  })
  
  
  output$vcf_count <- renderInfoBox({
    infoBox(
      title = "Samples in vcf", value = as.character(tbl(con, "unique_ena_run_summary") %>% filter(table_name == "vcf") %>% select(count) %>% collect()),
      icon = icon("circle"),
      color = "yellow"
    )
  })
  
  
  
  output$cov_count <- renderInfoBox({
    infoBox(
      title = "Samples in cov", value = as.character(tbl(con, "unique_ena_run_summary") %>% filter(table_name == "cov") %>% select(count) %>% collect()),
      icon = icon("circle"),
      color = "yellow"
    )
  })
  
  
  output$meta_count <- renderInfoBox({
    infoBox(
      title = "Samples in meta", value = as.character(tbl(con, "unique_ena_run_summary") %>% filter(table_name == "meta") %>% select(count) %>% collect()),
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
    x <- lineage2 %>%
      dplyr::filter(Country == input$selected_country) %>%
      arrange(clean_collection_date)
    
    
    highchart(type = "stock") %>%
      hc_add_series(x, "scatter", hcaes(x = clean_collection_date, y = pct, group = variant_id),
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
      hc_scrollbar(enabled = FALSE)
  })
  
  
  output$distPlot_lineage_lineage <- renderHighchart({
    x <- lineage %>%
      dplyr::filter(variant_id == input$selected_lineage) %>%
      arrange(clean_collection_date)
    
    
    highchart(type = "stock") %>%
      hc_add_series(x, "scatter", hcaes(x = clean_collection_date, y = n, group = Country),
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
      hc_scrollbar(enabled = FALSE)
  })
  
  
  
  output$variant_weekly <- renderHighchart({
    x <- variant_master_table %>%
      dplyr::filter(country_name == input$selected_country_for_variants) %>%
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
      hc_xAxis(categories = unique(x$date))
  })
  
  
  
  output$table <- DT::renderDataTable(
    DT::datatable(
      {
        lineage_def %>%
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
