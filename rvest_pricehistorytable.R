library(rvest)
library(tidyverse)


### set up date sequence
dates <- data.frame(seq(as.Date("2015-07-01"), Sys.Date()-1, by = "days"))
names(dates)[1] <- 'date'


### pull in historical MXN exchange rate data from csv, mutate date from chr to date, and change column name
hist_mxn <- read_csv("2015-2021.csv") %>% 
              mutate(., date = readr::parse_date(date, "%m/%d/%Y"))
names(hist_mxn)[2] <- 'hist_mxn'



### scrape all necessary gold price data -- USD per oz.
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



# join dates and gold_price, fill in NA data with previous days' data.
gold_price <- left_join(dates,
                        gold_price,
                        by = "date") %>% 
              fill(`Closing Price`) %>%
              rename(gold_usd_oz = 'Closing Price')


head(gold_price)



### scrape current USD/MXN data

page <- "https://finance.yahoo.com/quote/USDMXN%3DX/history?period1=1070150400&period2=1647475200&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true"

current_mxn <- as.data.frame(
  read_html(page) %>%
  html_nodes("table") %>%
  html_table
  )

#parse_date to ISO, and mxn to double.
current_mxn <- mutate(current_mxn, date = parse_date(Date, "%b %d, %Y"), mxn = parse_double(Close.))


head(current_mxn)




### left_join historical MXN data with gold price (in USD per oz), and then left_join with current MXN data.
gold_mxn <- left_join(gold_price, hist_mxn, by = 'date') %>%
              left_join(., current_mxn %>% select(date, mxn), by = 'date')


# if mxn column is NA, replace with hist_mxn, otherwise keep mxn.
gold_mxn$mxn <- ifelse(is.na(gold_mxn$mxn), gold_mxn$hist_mxn, gold_mxn$mxn)


# fill all NA's with previous date's price, add mxn_per_gram column, and drop hist_mxn column
gold_mxn <- gold_mxn %>% fill(mxn)
gold_mxn$mxn_per_gram <- ((gold_mxn$gold_usd_oz / 28.3495) * gold_mxn$mxn)
gold_mxn = select(gold_mxn, -hist_mxn) 





### finally.
head(gold_mxn)

ggplot(gold_mxn, aes(x = date, y = mxn_per_gram)) +
  geom_line()



