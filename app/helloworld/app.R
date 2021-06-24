library(shiny)
library(DBI)
library(shinydashboard)
library(tidyverse)
library(pool) 
library(config)



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


ui <- dashboardPage(
    
    dashboardHeader(title = "Test"),
    
    dashboardSidebar(
        sidebarMenu(id="sidebar",
                    menuItem("Info", tabName = "menu_info")
        )
    ),
    
    dashboardBody(
        tabItems(
            
          tabItem(tabName = "menu_info",
                  fluidRow(
                    DT::dataTableOutput("vcf_head")
                  )
          )
        )
    )
)



server <- function(input, output) {
  output$vcf_head = DT::renderDataTable({
    DT::datatable(
      { tbl(con, "vcf") %>%
          head()%>%
          collect()},
      #selection = 'single',
      #extensions = 'Buttons',
      options = list(lengthChange = FALSE, 
                     paging = FALSE,
                     dom = 't')
      
    )
  })
    
}

# Run the application 
shinyApp(ui = ui, server = server)


