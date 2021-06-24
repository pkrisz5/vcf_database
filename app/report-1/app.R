library(shiny)
library(DBI)
library(shinydashboard)
library(tidyverse)
library(lubridate)
library(plotly)
library(highcharter)
library(pool) 

library(config)
#library(ISOweek)
#library(NGLVieweR)

app_version <- "v_000.010"

# lapply(dbListConnections(drv = dbDriver("PostgreSQL")), function(x) {dbDisconnect(conn = x)})

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
    #poolClose(con2)
    poolClose(con)
})



lineage <- tbl(con, "meta") %>%
    filter(clean_host=="Homo sapiens")%>%
    select(ena_run:clean_country)%>%
    inner_join(tbl(con, "lineage"))%>%
    select(ena_run:variant_id)%>% 
    dplyr::filter(collection_date>as_date("2020-01-01")) %>%
    group_by(collection_date, clean_country, variant_id)%>%
    dplyr::summarise(n=n())%>%
    collect()%>%
    drop_na()%>%
    dplyr::rename(Country="clean_country")
    

############################################################################################
# User interface of the app
############################################################################################

# Define UI for application that draws a histogram
ui <- dashboardPage(
    
    dashboardHeader(title = "VCF database"),
    
    dashboardSidebar(
        sidebarMenu(id="sidebar",
                    menuItem("Samples from countries", tabName = "country_graph", icon = icon("chart-bar"))
                    
        )
    ),
    
    
    
    

    
    
    dashboardBody(
        tabItems(
            
            tabItem(tabName = "country_graph",
                    tags$h4("The graph below shows how many samples were sequenced and sent to EBI in a gived day in a given EU state"),        
                                
                                
                                box( title = "",
                                     status = "primary",
                                     height = "450", width = "12",solidHeader = FALSE,
                                     column(width = 12,
                                            highchartOutput("distPlot"),
                                     )
                                )
                       )
                       
                           

                    ),
                    
                    
                    
            ),
            
            
            
 
            
            
            
            )


# Define server logic required to draw a histogram
server <- function(input, output) {
    
    
    output$distPlot <- renderHighchart({
        eu <- c("United Kingdom", "Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus", "Czech Republic", "Denmark", "Estonia", "Finland", "France", "Germany", "Greece", "Hungary",
                "Ireland", "Italy", "Latvia", "Lithuania", "Luxembourg", "Malta", "Netherlands", "Poland", "Portugal", "Romania", "Slovakia", "Slovenia", "Spain", "Sweden")
        
        x <- tbl(con, "meta") %>%
            dplyr::filter(clean_host=="Homo sapiens") %>%
            group_by(clean_country, collection_date) %>%
            dplyr::summarise(n=n()) %>%
            collect()%>%
            drop_na()%>%
            arrange(desc(n)) %>%
            dplyr::filter(clean_country%in%local(eu)) 
        
        x <- x%>%
            dplyr::filter(collection_date>as_date("2020-01-06")) %>%
            dplyr::rename(Country="clean_country")%>%
            group_by(Country)%>%
          arrange(collection_date)

        
        highchart(type="stock")%>%
          hc_add_series(x, "scatter", hcaes(x=collection_date, y=n, group=Country),
                        tooltip = list(pointFormat = "Number of samples sequenced on a given day in {point.Country}:{point.n}: ")) %>%
          hc_title(text = "Number of samples derived from EU states")%>%
          hc_legend(enabled = TRUE,
                    title = list(text = "Select/deselect countries:"))%>%
          hc_yAxis(title = list(text = "Analysed sample / new cases * 100")) %>%
          hc_tooltip(crosshairs = TRUE) %>%
          hc_navigator(enabled = FALSE) %>%
          hc_scrollbar (enabled = FALSE)
 
    })
    
}

# Run the application 
shinyApp(ui = ui, server = server)



