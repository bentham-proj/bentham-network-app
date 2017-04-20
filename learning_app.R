library(shiny)
library(shinydashboard)
library(purrr)
library(dplyr)
library(tidyr)
library(stringr)
library(DT)
install.packages("DT", dependencies = TRUE)
install.packages("ggvis", dependencies = TRUE)

DT::datatable(df)

getwd()
df <- mtcars %>% as_data_frame()
df$text <- sample(c("prison rocks", "yo bro, here is the sort code 09-23-12", "get the spice man he has all the best spice, the best spice you can get in all the land. I am in a gang also"), 
                  size = 32, 
                  replace = TRUE)

ui <- dashboardPage(
  dashboardHeader(title = "Mobile phone data"),
  
  dashboardSidebar(
    
    dashboardSidebar(
      sidebarMenu(
        menuItem("Dashboard", tabName = "dashboard", icon = icon("dashboard")),
        menuItem("Phone networks", tabName = "phone_networks", icon = icon("group")),
        
        fileInput("datacsv", "Select input file (.csv)"), 
        
        selectInput("column1", 
                    label = "Gear:",
                    choices = NULL,
                    multiple = TRUE, 
                    selected = ), 
        selectInput("column2", 
                    label = "Cylinder", 
                    choices = NULL, 
                    multiple = TRUE), 
        textInput("column3", 
                  label = "Search text", 
                  placeholder = "Search..."),
        sliderInput("n_rows", 
                    label = "Select number of rows to view", 
                    min = 1, 
                    max = 100, 
                    value = 20), 
        downloadLink("data_download", "Extract selected data")
      )
    )
  ), 
  
  dashboardBody(
    tabItems(
      tabItem(tabName = "dashboard", h2("Data viewer"),
              fluidPage(box(dataTableOutput("mtcarsdata")))), 
      
      tabItem(tabName = "phone_networks", 
              h2("Widgets tab content")
      )
    )
  )
)

server <- shinyServer(function(input, output, session) {
  
  mydata <- df 
  
  updateSelectInput(session, 
                    inputId = "column1", 
                    choices = unique(mydata$gear), 
                    selected = sample(unique(mydata$gear), 1))
  
  updateSelectInput(session,
                    inputId = "column2", 
                    choices = unique(mydata$cyl), 
                    selected = sample(unique(mydata$cyl), 1))
  
  output$mtcarsdata <- renderDataTable(options = list(
    searchHighlight = TRUE,
    # this adds black colour to col header
    initComplete = JS(
      "function(settings, json) {",
      "$(this.api().table().header()).css({'background-color': '#000', 'color': '#fff'});",
      "}"),
    columnDefs = list(list(
      targets = 12,
      render = JS(
        "function(data, type, row, meta) {",
        "return type === 'display' && data.length > 6 ?",
        "'<span title=\"' + data + '\">' + data.substr(0, 6) + '...</span>' : data;",
        "}")
    ))), callback = JS('table.page(3).draw(false);'), {
      
      in1  <- input$column1
      in2  <- input$column2
      in3  <- input$column3
      rows <- input$n_rows
      
      search_term <- paste0(in3, collapse = "|")
      text_search_result <- str_detect(mydata$text, search_term)
      
      mydata[mydata$gear %in% in1 & 
               mydata$cyl %in% in2 & 
               text_search_result, ][0:rows, ]
      
    })
  
  output$data_download <- downloadHandler(
    
    filename = function() {paste("data-", Sys.Date(), ".csv", sep="")},
    content = function(file) {write.csv(output$mtcarsdata, file)}
    
  )
})

if (interactive()) {
  shinyApp(ui, server)
}
