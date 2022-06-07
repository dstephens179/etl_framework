# LIBRARIES ----
library(DataExplorer)
library(tidyverse)
library(timetk)
library(lubridate)
library(bigrquery)
library(googlesheets4)



# FILTER ----
# set filter
date_filter <- '2022-06-01'



# DATA ----

# Data from Big Query ----

projectid = "source-data-314320"
sql <- "SELECT *
        FROM `source-data-314320.Store_Data.All_Data`
        ORDER BY Date desc
"



# Run the query and store
bq_query <- bq_project_query(projectid, sql)
data_tbl <- bq_table_download(bq_query)



# Split historical & forecast
data_forecast_tbl     <- data_tbl %>% filter(forecast > 0)
data_historical_tbl   <- data_tbl %>% filter(sales != 0) %>% filter(date < date_filter)


write_csv(x = data_tbl, file = "historical_store_data/All_Data_until_2022-05-31.csv")




# Current Data from GSheets ----

# Set id's
centro_id   <- '1RedLhHkxn1YzmQesvhLx72uP2MarX3KOwMSuBBl6eDc'
segovia_id  <- '1Kd2ssnHU5dl5sCFId2CQK2jPMZTliKZl_M85miNYkVA'
patria_id   <- '1GsW1IX-qAffeUnCjQhNZvSTfaOHDFWm5qHTyf4SGKrY'
pasaje_id   <- '14_IV7eV_a9FO1V69Y9YZnVszFBFm-A6tuMxXDUvUEpY'
vallardo_id <- '1DP06x3qH2cxNxcJnSfRSLpVPRHZja5qQCYxrJ-nCyD8'
va_id       <- '14X9H73Yyo1VCmIwZgIkgKz3gUhpP8oM76B9CVjGSqnE'


# Set select_filter
select_filter <- c('tienda', 'may_men', 'date', 
                   'owner', 'metal_type', 'linea', 
                   'product_type', 'sales', 'forecast', 
                   'inventario', 'nombre_cliente', 'nombre_agente')


# * Centro ----
centro_prepared_tbl <- read_sheet(ss = centro_id, 
                                  sheet = "Sales Data", 
                                  .name_repair = make.names) %>%
  mutate(tienda         = 'Centro', 
         may_men        = MAY.MEN,
         date           = ymd(Date), 
         owner          = Owner, 
         metal_type     = Metal.type,
         linea          = NA,
         product_type   = Product.Type, 
         sales          = Sales,
         forecast       = NA,
         inventario     = INVENTARIO,
         nombre_cliente = NOMBRE.CLIENTE,
         nombre_agente  = NOMBRE.AGENTE) %>%
  select(select_filter) %>%
  filter(date >= date_filter)




# * Segovia ----
segovia_prepared_tbl <- read_sheet(ss = segovia_id, 
                                   sheet = "Sales Data", 
                                   .name_repair = make.names) %>%
  mutate(tienda         = 'Segovia', 
            may_men        = MAY.MEN,
            date           = ymd(Date), 
            owner          = Owner, 
            metal_type     = Metal.type,
            linea          = Linea.Alineada,
            product_type   = Product.Type, 
            sales          = Sales,
            forecast       = NA,
            inventario     = INVENTARIO,
            nombre_cliente = NOMBRE.CLIENTE,
            nombre_agente  = NOMBRE.AGENTE) %>%
  select(select_filter) %>%
  filter(date >= date_filter)




# * Pl.Patria ----
patria_prepared_tbl <- read_sheet(ss = patria_id, 
                                  sheet = "Sales Data", 
                                  .name_repair = make.names) %>%
  mutate(tienda         = 'Pl.Patria', 
         may_men        = MAY.MEN,
         date           = ymd(Date), 
         owner          = Owner, 
         metal_type     = Metal.type,
         linea          = NA,
         product_type   = Product.Type, 
         sales          = Sales,
         forecast       = NA,
         inventario     = INVENTARIO,
         nombre_cliente = NOMBRE.CLIENTE,
         nombre_agente  = NOMBRE.AGENTE) %>%
  select(select_filter) %>%
  filter(date >= date_filter)





# * Pasaje ----
pasaje_prepared_tbl <- read_sheet(ss = pasaje_id, 
                                  sheet = "Sales Data", 
                                  .name_repair = make.names) %>%
  mutate(tienda         = 'Pasaje',
         may_men        = MAY.MEN,
         date           = ymd(FECHA), 
         owner          = Owner, 
         metal_type     = Metal.type,
         linea          = Linea.Alineada,
         product_type   = Product.Type, 
         sales          = IMPORTE,
         forecast       = NA,
         inventario     = INVENTARIO,
         nombre_cliente = NOMBRE.CLIENTE,
         nombre_agente  = NOMBRE.AGENTE) %>%
  select(select_filter) %>%
  filter(date >= date_filter)




# * Vallardo ----
vallardo_prepared_tbl <- read_sheet(ss = vallardo_id, 
                                    sheet = "Sales Data", 
                                    .name_repair = make.names) %>%
  mutate(tienda         = 'Vallardo',
         may_men        = MAY.MEN,
         date           = ymd(FECHA), 
         owner          = 'Jorge', 
         metal_type     = Metal.type,
         linea          = NA,
         product_type   = Product.Type, 
         sales          = IMPORTE,
         forecast       = NA,
         inventario     = INVENTARIO,
         nombre_cliente = NOMBRE.CLIENTE,
         nombre_agente  = NOMBRE.AGENTE) %>%
  select(select_filter) %>%
  filter(date >= date_filter)




# * VA ----
va_prepared_tbl <- read_sheet(ss = va_id, 
                              sheet = "Sales Data", 
                              .name_repair = make.names) %>%
  mutate(tienda         = Tienda,
         may_men        = MAY.MEN,
         date           = ymd(Date), 
         owner          = 'Alex', 
         metal_type     = Metal.Type,
         linea          = Linea.Alineada,
         product_type   = Product.Type, 
         sales          = IMPORTE,
         forecast       = NA,
         inventario     = Piezas,
         nombre_cliente = Nombre.Cliente,
         nombre_agente  = Nombre.Agente) %>%
  select(select_filter) %>%
  filter(date >= date_filter)





# Append all current data ----

appended_sales_tbl <- bind_rows(data_historical_tbl,
                                data_forecast_tbl,
                                centro_prepared_tbl,
                                segovia_prepared_tbl,
                                patria_prepared_tbl,
                                pasaje_prepared_tbl,
                                vallardo_prepared_tbl,
                                va_prepared_tbl)



# left join gold & mxn

full_dataset_tbl <- left_join(x = appended_sales_tbl, 
                              y = gold_mxn, 
                              by = 'date')



# Upload to Big Query ----

datasetid <- "source-data-314320.Store_Data.All_Data"
# datasetid <- "source-data-314320.Store_Data.dev_all_data"



bq_perform_upload(datasetid,
                  full_dataset_tbl,
                  fields = list(bq_field(name = "tienda", type = "string"),
                                bq_field(name = "may_men", type = "string"),
                                bq_field(name = "date", type = "date"),
                                bq_field(name = "owner", type = "string"),
                                bq_field(name = "metal_type", type = "string"),
                                bq_field(name = "linea", type = "string"),
                                bq_field(name = "product_type", type = "string"),
                                bq_field(name = "sales", type = "float"),
                                bq_field(name = "forecast", type = "float"),
                                bq_field(name = "inventario", type = "float"),
                                bq_field(name = "nombre_cliente", type = "string"),
                                bq_field(name = "nombre_agente", type = "string"),
                                bq_field(name = "gold_usd_oz", type = "float"),
                                bq_field(name = "mxn", type = "float"),
                                bq_field(name = "mxn_per_gram", type = "float")),
                  nskip = 0,
                  source_format = "CSV",
                  create_disposition = "CREATE_IF_NEEDED",
                  write_disposition = "WRITE_TRUNCATE")




