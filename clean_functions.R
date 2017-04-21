"this file cleans all of the extracted data and saves new clean dataframes 
as .csv files"

library(dplyr)
library(tidyr)
library(purrr)
library(janitor)
library(magrittr)
library(readr)
library(stringr)
library(lubridate)

options(dplyr.width = Inf)
wd <- "C:\\Users\\Chris\\Desktop\\extracted_phone_465\\extracted_data"
setwd(wd)

cl_phone_nums <- function(phnum_col) {
  "Extract only digits from phone number columns"
  phnum <- str_extract(phnum_col, "[[:digit:]]+") 
  phnum <- str_replace(phnum, "([44])+", "0")
  phnum
}

cl_to_datetime <- function(timecol) {
  "Create datetime object from 'time' column"
  
  timecol <- str_replace_all(timecol, "\\(Device\\)", "") %>% 
    str_replace_all(., "\\(Network\\)", "") %>% 
    str_replace_all(., "UTC", "") %>% str_trim() %>% 
    str_replace_all(., "/", "-") %>% 
    dmy_hms()
  
  return(timecol)
  
}

#### clean exhibit data #######################################################

clean_exhibit_data <- function(exhib_raw_df) {
  
  exhib_raw_df <- exhib_raw_df %>% 
    map_df(~na_if(., "N.A")) %>% 
    map_df(~na_if(., "N A")) %>% 
    map_df(~na_if(., "Na")) %>% 
    map_df(~na_if(., "UNKNOWN")) %>% 
    map_df(~na_if(., "NOT VIEWABLE"))
  
  # 293 phones are attributable
  exhib_raw_df$attributable <- ifelse(is.na(exhib_raw_df$inmates_name) & 
                                      is.na(exhib_raw_df$inmates_name_1), FALSE, TRUE)
  
  # consolidate the four cols for inmates_name and inmates_no into two
  ## issue with names
  # some show as "OGUNLOYE = OGBOGBO" or "CASSELL = MARSHALL", why? others as
  # HAYES/RIDGEWAY
  exhib_raw_df$prisoner_name    <- ifelse(!is.na(exhib_raw_df$inmates_name), 
                               exhib_raw_df$inmates_name, exhib_raw_df$inmates_name_1)
  
  ## issue with numbers 
  # some show as A3930DA / A3375CY others as A5893CQ = A1729DP or A1473CK, A0992A
  exhib_raw_df$prisoner_number <- ifelse(!is.na(exhib_raw_df$inmates_no), 
                               exhib_raw_df$inmates_no, exhib_raw_df$inmates_no_1)
  
  # case_reference has three different entries for BRIXTON: "BRIXTON", "1BRIXTON" 
  # and "BRIXTON1"
  exhib_raw_df$prison <- str_extract(exhib_raw_df$case_reference, "[a-zA-Z]+") %>% 
    unlist()
  
  exhib_raw_df$estab_bag_no <- ifelse(is.na(exhib_raw_df$estab_bag_no), 
                                    exhib_raw_df$estab_bag_no_1, exhib_raw_df$estab_bag_no)
  
  
  # columns that are useful and not mostly NA
  useful_exhibit_cols <- c("exhibit_id", "attributable", "case_operator",
                           "imei", "important_items_1", "notes", "prisoner_name", "prisoner_number", 
                           "estab_bag_no", "prison", "exhibit_type")
  
  exhib_raw_df$exhibit_type <- ifelse(str_detect(
    str_sub(exhib_raw_df$exhibit_id, start = -2,end = -1), "S"),  "sim", "phone")
  
  exhib_raw_df <- exhib_raw_df[, useful_exhibit_cols]

  exhib_raw_df

}

#### clean summary data ######################################################

clean_summary_data <- function(summary_data_raw) {
  
  summary_useful_columns <- c("file_name", "date_created", "extraction_media", 
                              "exhibit_id")
  
  summary_data_raw$date_created <- cl_to_datetime(summary_data_raw$date_created)
  #summary_data_raw <- new_ymdh(summary_data_raw, timecol = summary_data_raw$date_created)
  
  summary_data_raw[, summary_useful_columns]
  
}

#### clean gdi data ###########################################################

clean_gdi_data <- function(gdi_data_raw) {
  
  
  gdi_data_raw <- gdi_data_raw %>% clean_names()
  gdi_data_raw$number <- cl_phone_nums(gdi_data_raw$number)
  
  useful_gdi_cols <- c("exhibit_id", "file_name", "device_family", "device_name", 
                       "issuer_id_from_iccid", "number")
  
  
  gdi_data_raw[, useful_gdi_cols]
  
  
}

#### apply to exhibit, summary and gdi data ####################################

exhibit_data <- read_csv("exhibit_data.csv") %>% clean_exhibit_data()
summary_data <- read_csv("summary_data.csv") %>% clean_summary_data
gdi_data     <- read_csv("general_device_info.csv") %>% clean_gdi_data()

# create phone_overview file consolidating all of these dataframes

exhibit_overview <- inner_join(exhibit_data, summary_data) %>%
  inner_join(gdi_data) 



# there are some duplicate entries. On inspection, it seems like 
# these are from multiple inputs of the same case by the operator at the prison.

exhibit_overview <- distinct(exhibit_overview, exhibit_id, file_name, .keep_all = TRUE)


ph_overview_useful <- c("exhibit_id", "file_name", "attributable", "prisoner_name", 
                "prisoner_number", "prison", "date_created", "number")

exhibit_overview$prison_code <- str_sub(exhibit_overview$exhibit_id, 1, 2)
exhibit_overview$parent_exhibit_id <- substr(exhibit_overview$exhibit_id, 1, 7)

exhibit_overview$month_created <- lubridate::month(exhibit_overview$date_created, label = TRUE)
exhibit_overview$day_of_month_created <- lubridate::days_in_month(exhibit_overview$date_created) 

# here we add a general indicator of how many exhibits are associated with the 
# 7 digit exhibit id
exhibit_overview <- exhibit_overview %>% 
  group_by(parent_exhibit_id) %>% 
  mutate(n_related_exhibits = n_distinct(exhibit_id)) %>% 
  ungroup() 

#### clean contacts data #######################################################


contacts_data <- read_csv("contacts_data.csv")

contacts_data$tel <- cl_phone_nums(contacts_data$tel)

# if you filter out phone numbers with less than 5 digits you remove a lot of 
# the standard network contacts etc., but you also lose contact names that 
# do not have a full number.

contacts_usefulnums_for_networking <- contacts_data %>% filter(nchar(tel) > 7)

###############################################################################
#### functions for cleaning call and text data  ######
###############################################################################


new_sent_recieve_col <- function(fromcol) {
  "Create new column specifying the direciton of the call. If the 'from' column
  is NA then it means that the call was outgoing. Therefore, if from columns ==
  NA, then new column shows 'out' otherwise 'in', meaning call being recieved.
  "
  
  ifelse(is.na(fromcol), "out", "in")
  
}

new_ymdh <- function(df, timecol) {
  "Creates new year, month, day and hour columns."
  
  df %>% mutate(
    year = year(timecol), 
    month = month(timecol),
    day = day(timecol),
    hour = hour(timecol)
  )
  
}

new_all_numbers_calls <- function(df) {
  
  "find the rows where both from and to have phone numbers. Then add both
  numbers into a single cell. Then seperate this cell into two rows (keeping)
  all other values the same in the two rows. Then find na values in from and
  If there is no number in the from column (because it is an outgoing call 'to' someone), 
  insert the number that is in the 'to' column."
  
  df$from  <- str_trim(df$from) %>% str_replace_all(., " ", "")
  df$to  <- str_trim(df$to) %>% str_replace_all(., " ", "")
  
  df$all_nums  <- ifelse(!is.na(df$from) & !is.na(df$to), 
                         paste(df$from, "/", df$to), 
                         ifelse(is.na(df$from), df$to, 
                                ifelse(is.na(df$to), df$from, df$to)))
  
  df <- separate_rows(df, all_nums, sep = "/")
  
  df$direction <- ifelse(!is.na(df$from) & !is.na(df$to), df$call_type, 
                         ifelse(is.na(df$from), "out", "in"))
  
  df$direction <- ifelse(df$direction == "Received", "in", df$direction)
  df$direction <- ifelse(df$direction == "Dialed", "out", df$direction)
  df$all_nums  <- str_trim(df$all_nums) %>% str_replace_all(., " ", "")
  
  df
  
}

new_all_numbers_texts <- function(df) {
  
  "find the rows where both from and to have phone numbers. Then add both
  numbers into a single cell. Then seperate this cell into two rows (keeping)
  all other values the same in the two rows. Then find na values in from and
  If there is no number in the from column (because it is an outgoing call 'to' someone), 
  insert the number that is in the 'to' column."
  
  
  df$from  <- str_trim(df$from) %>% str_replace_all(., " ", "")
  df$to  <- str_trim(df$to) %>% str_replace_all(., " ", "")
  
  df$all_nums  <- ifelse((!is.na(df$from) & !is.na(df$to)), 
                         paste(df$from, "/", df$to), 
                         ifelse(is.na(df$from), df$to, 
                                ifelse(is.na(df$to), df$from, df$to)))
  
  df <- separate_rows(df, all_nums, sep = "/")
  
  df$direction <- ifelse(is.na(df$from), "out", "in")
  
  df$all_nums  <- str_trim(df$all_nums) %>% str_replace_all(., " ", "")
  
  df
  
}

cl_filter_not_na_notnum <- function(df) {
  
  df %>% filter(!is.na(all_nums) & nchar(all_nums) > 7)
  
  
}

clean_df_callstexts <- function(df, type) {
  "df is either calls or texts df, specify in type arg
  type = 'texts'. This function applys all functions to the specified dataframe.
  "
  
  df$type      <- type
  df$from      <- cl_phone_nums(df$from)
  df$to        <- cl_phone_nums(df$to)
  
  if (type == "call") {
    df <- new_all_numbers_calls(df) }
  
  else {
    
    df <- new_all_numbers_texts(df)
    
  }
  
  df$time <- cl_to_datetime(df$time)
  df$file_name <- NULL
  
  #df <- new_ymdh(df, df$time)
  cl_filter_not_na_notnum(df)
  
  
  
}

newdf_all_interactions <- function(callsdf, textsdf) {
  "Create new dataframe with all interactions. That is, the call and texts for
  each phone combined. This function returns a cleaned dataframe with NAs and 
  numbers that are not phone numbers (those less than 7 digits) removed
  "
  
  callsdf$type      <- "call"
  callsdf$direction <- new_sent_recieve_col(callsdf$from)
  
  
  textsdf$type = "text"
  textsdf$direction <- new_sent_recieve_col(textsdf$from)
  
  
  calls_sub <- callsdf %>% select(exhibit_id, all_nums, type, direction, time)
  texts_sub <- textsdf %>% select(exhibit_id, all_nums, type, direction, time)
  
  bind_rows(calls_sub, texts_sub)
  
}

#### apply functions to text and call data. create all_interactions df #########

calls_data <- read_csv("calls_data.csv") %>% clean_df_callstexts(., "call")
texts_data <- read_csv("sms_data.csv") %>% clean_df_callstexts(., "text")

all_interactions <- newdf_all_interactions(calls_data, texts_data)

#### save all dataframes ######################################################
calls_data
newwd <- "C:\\Users\\Chris\\Desktop\\extracted_phone_465\\clean_csv_data"
setwd(newwd)

write_csv(exhibit_data, "exhibit_data_clean.csv")
write_csv(summary_data, "summary_data_clean.csv")
write_csv(gdi_data, "gen_device_data_clean.csv")
write_csv(exhibit_overview, "exhibit_overview.csv")
write_csv(calls_data, "calls_data_clean.csv")
write_csv(texts_data, "texts_data_clean.csv")
write_csv(all_interactions, "all_interactions.csv")
write_csv(contacts_data, "contacts_data_clean.csv")
write_csv(contacts_usefulnums_for_networking, "contacts_usefulnums_for_networking.csv")



