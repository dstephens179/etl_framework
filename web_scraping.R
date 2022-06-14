# Libraries ----
library(rvest)
library(tidyverse)
library(plotly)
library(bigrquery)



# Set dates ----
dates <- data.frame(seq(as.Date("2015-07-01"), Sys.Date()-1, by = "days"))
names(dates)[1] <- 'date'



# DATA ----
# tidy up historical data
hist_mxn <- read_csv("historical_MXN_USD\\2015-2021.csv") %>% 
              mutate(., date = readr::parse_date(date, "%m/%d/%Y"))
names(hist_mxn)[2] <- 'hist_mxn'



# web scrape all gold price data -- USD per oz.
scrape <- function(year){
  
  url <- paste("https://www.usagold.com/daily-gold-price-history/?ddYears=", year, sep = "")
  
  cat("Scraping url: ", url, "\n")
  
  df <- read_html(url) %>%
    html_nodes("#pricehistorytable") %>%
    html_table
  
  return(df)
  
  Sys.sleep(1)  # pauses for 1 second.
  
}



# * Scrape gold, reformat & clean ----
gold_price <- map_df(seq(2015, 2022), scrape)
gold_price <- mutate(gold_price, date = readr::parse_date(date, "%d %b %Y"))
gold_price <- gold_price[rev(order(gold_price$date)),] %>%
                na.omit(gold_price)



# * Left join and fill NA's ----
gold_price <- left_join(dates,
                        gold_price,
                        by = "date") %>% 
              fill(`Closing Price`) %>%
              rename(gold_usd_oz = 'Closing Price')




# * Scrape current USD/MXN data ----

page <- "https://finance.yahoo.com/quote/USDMXN%3DX/history?period1=1070064000&period2=1800000000&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true"

current_mxn <- as.data.frame(
  read_html(page) %>%
  html_nodes("table") %>%
  html_table
  )


# * Clean up data ----
current_mxn <- mutate(current_mxn, date = parse_date(Date, "%b %d, %Y"), mxn = parse_double(Close.))



# * Left join and fill NA's ----
### left_join historical MXN data with gold price (in USD per oz), and then current MXN data.
gold_mxn <- left_join(gold_price, hist_mxn, by = 'date') %>%
              left_join(., current_mxn %>% select(date, mxn), by = 'date')


# if mxn column is NA, replace with hist_mxn, otherwise keep mxn.
gold_mxn$mxn <- ifelse(is.na(gold_mxn$mxn), gold_mxn$hist_mxn, gold_mxn$mxn)


# fill all NA's with previous date's price, add mxn_per_gram column, and drop hist_mxn column
gold_mxn <- gold_mxn %>% fill(mxn)
gold_mxn$mxn_per_gram <- ((gold_mxn$gold_usd_oz * 0.0321507) * gold_mxn$mxn)
gold_mxn = select(gold_mxn, -hist_mxn) 


# remove duplicate dates from overlapping years
gold_mxn <- gold_mxn[!duplicated(gold_mxn$date), ]


# finally.  
View(gold_mxn)



# VISUALIZE ----
# Gold Price per Gram in MXN
plot_ly(
  gold_mxn, 
  x = ~gold_mxn$date, 
  y = ~gold_mxn$mxn_per_gram, 
  type = 'scatter', 
  mode = 'lines') %>%
  layout(
    title = 'Gold Price per Gram (MXN)', 
    xaxis = list(title = 'Date'), 
    yaxis = list(title = 'MXN per Gram'))


# USD in MXN chart
plot_ly(gold_mxn,
        x = ~gold_mxn$date,
        y = ~gold_mxn$mxn,
        type = 'scatter',
        mode = 'lines') %>%
  layout(title = 'USD/MXN Exchange Rate', 
         xaxis = list(title = 'Date'), 
         yaxis = list(title = 'USD in MXN'))




# UPLOAD ----
# upload web-scraped data to bigquery.

datasetid <- "source-data-314320.joyeria_dataset.gold_price"

# use bigrquery to create (if needed), upload and overwrite the dataset
bq_perform_upload(datasetid,
                  gold_mxn,
                  nskip = 0,
                  source_format = "CSV",
                  create_disposition = "CREATE_IF_NEEDED",
                  write_disposition = "WRITE_TRUNCATE")



