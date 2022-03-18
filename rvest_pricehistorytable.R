library(rvest)
library(tidyverse)


### set up date sequence
dates <- data.frame(seq(as.Date("2015-07-01"), Sys.Date()-1, by = "days"))
names(dates)[1] <- 'date'



### scrape gold price data -- USD per oz.
scrape <- function(year){
  
  url <- paste("https://www.usagold.com/daily-gold-price-history/?ddYears=", year, sep = "")
  
  cat("Scraping url: ", url, "\n")
  
  df <- read_html(url) %>%
    html_nodes("#pricehistorytable") %>%
    html_table
  
  return(df)
  
  Sys.sleep(1)  # pauses for 1 second.
  
}

gold_price <- map_df(seq(2015, 2022), scrape)
gold_price <- mutate(gold_price, date = readr::parse_date(date, "%d %b %Y"))
gold_price <- gold_price[rev(order(gold_price$date)),] %>%
                na.omit(gold_price)



# join dates and gold_price, and fill in NA data with previous days' data.
gold_price <- left_join(dates,
                        gold_price %>% fill(`Closing Price`),
                        by = "date")


head(gold_price)



### scrape USD/MXN data from 

page <- "https://finance.yahoo.com/quote/USDMXN%3DX/history?period1=1070150400&period2=1647475200&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true"

usd_mxn <- as.data.frame(
  read_html(page) %>%
  html_nodes("table") %>%
  html_table
  )


usd_mxn <- mutate(usd_mxn, date = parse_date(Date, "%b %d, %Y"), mxn = parse_double(Close.))







gold_mxn <- left_join(gold_price,
                      usd_mxn %>% select(date, mxn),
                      by = "date")






View(gold_mxn)