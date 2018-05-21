library(httr)
library(jsonlite)
library(lubridate)
library(plyr)
library(googlesheets) 
suppressMessages(library(dplyr))


##Functions
#is.odd <- function(x) x %% 2 != 0 

##Determine the start of bi-weekly iterations
today <- Sys.Date()
#previousMonday <- 7 * floor(as.numeric(today-1+4) / 7) + as.Date(1-4,origin = "1970-01-01")
#weekNumPreviousMonday <- week(previousMonday)
#MondayTwoWeeksAgo <- 7 * floor(as.numeric(today-7-1+4) / 7) + as.Date(1-4,origin = "1970-01-01")
#j <- ifelse(is.odd(weekNumPreviousMonday)=='TRUE', previousMonday, MondayTwoWeeksAgo)

##Determine the start of monthly iterations
YTD <- floor_date(Sys.Date(), "year")

##for (i in 1:265) {
date <- today-9
  ##create empty dataframes
  date.date <- data.frame(date=as.Date(date, origin = "1970-01-01"),
                          stringsAsFactors=FALSE) 

  demo.booked <- data.frame(date=character(),
                           demoBooked=integer(), 
                           stringsAsFactors=FALSE) 
  
  demo.attended <- data.frame(date=character(),
                         demoAttended=integer(), 
                         stringsAsFactors=FALSE) 
  
  enrolled <- data.frame(date=character(),
                          enrolled=integer(), 
                          stringsAsFactors=FALSE) 
  
  
  ##Pull data from close.io
  ##Booked On
  url1 <- 'https://app.close.io/api/v1/report/custom/orga_yeUFxFqgMfYqqA6MafUKbHp4kyzD6icSdEjlf2TBr7E/?query=('
  url2 <- '”1-7:%20Demo%20Booked%20On”='
  url3 <- ')&x=lead.custom.1-3:%20ES&y=lead.count'
  token <- '2ffc8bc58fdaa14f3de041c6a77b35cc05c5a6dad65b11fa598ec894'
  getData <- GET(url = paste0(url1,url2,as.Date(date, origin = "1970-01-01"),url3), add_headers('X-TZ-offset' = -7), authenticate(token,':'))
  #content(getData)
  jsonRaw <- httr::content(getData, as = "text")
  json <- fromJSON(jsonRaw)
  tryCatch(demo.booked <- data.frame(date = date, demoBooked = unlist(json$series$count), updated=today), error=function(e){})
  
  
  ##Attended
  url1 <- 'https://app.close.io/api/v1/report/custom/orga_yeUFxFqgMfYqqA6MafUKbHp4kyzD6icSdEjlf2TBr7E/?query=("custom.1-8:%20Demo%20Attended"='
  url2 <- ')&x=lead.custom.1-3:%20ES&y=lead.count'
  token <- '2ffc8bc58fdaa14f3de041c6a77b35cc05c5a6dad65b11fa598ec894'
  getData <- GET(url = paste0(url1,as.Date(date, origin = "1970-01-01"),url2), add_headers('X-TZ-offset' = -7), authenticate(token,':'))
  content(getData)
  jsonRaw <- httr::content(getData, as = "text")
  json <- fromJSON(jsonRaw)
  tryCatch(demo.attended <- data.frame(date = date, demoAttended = unlist(json$series$count), updated=today), error=function(e){})
  
  ##Enrolled
  url1 <- 'https://app.close.io/api/v1/report/custom/orga_yeUFxFqgMfYqqA6MafUKbHp4kyzD6icSdEjlf2TBr7E/?query=(opportunity((status:"Churn"or%20status:"MANAGE%20enrolled"or%20status:"GROW%20enrolled"%20or%20status:"STARTER%20Enrolled"%20OR%20status:"SIMPLE%20Enrolled"or%20status:"MODERN%20Enrolled"%20or%20status:"ADVANCED%20Enrolled"%20or%20status:"Lost%20(on-boarding)")%20date_won='
  url2 <- '))&x=lead.custom.1-3:%20ES&y=lead.count'
  token <- '2ffc8bc58fdaa14f3de041c6a77b35cc05c5a6dad65b11fa598ec894'
  getData <- GET(url = paste0(url1,as.Date(date, origin = "1970-01-01"),url2), add_headers('X-TZ-offset' = -7), authenticate(token,':'))
  content(getData)
  jsonRaw <- httr::content(getData, as = "text")
  json <- fromJSON(jsonRaw)
  tryCatch(enrolled <- data.frame(date = date, enrolled = unlist(json$series$count), updated=today), error=function(e){})

  
  ##join data
  ##inbound <- join_all(list(new.booked,old.booked,future.booked,attended,completed,enrolled,calls,LIT), by="salesPerson", type = 'full')
  
  daily <- join_all(list(date.date,demo.booked,demo.attended,enrolled), by="date", type = 'full')
  
  ##Connect to Google Sheet
  DP_data <- gs_title("Daily Pulse Data")
  
  ##Refresh data of worksheets SP_Update and Team_Update
  DP_data <- DP_data %>%
    gs_add_row(ws = "Data", input = tail(daily))
##}
 

