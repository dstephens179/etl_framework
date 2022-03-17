library(rvest)
library(tidyverse)


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

head(gold_price)