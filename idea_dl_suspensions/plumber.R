# This is a simple API to call a script that preps data
# for KIPP Chicago's IDEA data reporting tool. This 
# endpoint is called by Airflow 

gcs_auth_file <- Sys.getenv("GCS_AUTH_FILE")
bq_auth_file <-Sys.getenv("BQ_AUTH_FILE")
bq_project <- Sys.getenv("BQ_PROJECT")

library(plumber)

library(dplyr)
library(lubridate)
library(purrr)
library(stringr)
library(googleCloudStorageR)
#library(jsonlite)
library(janitor)

library(silounloadr)

#* @apiTitle Deanslist Suspensions Prep Script



cat(sprintf("BQ_AUTH_FILE is %s", bq_auth_file))

bigrquery::set_service_token(bq_auth_file)

#* Trigger Deanslist Supsenions prep script for IDEA
#* @get /run_idea_dl_supsensions
function(res){
  
  cat("Get DL Supsesions")
  susps_raw <- get_deanslist("suspensions") %>% 
    select(suspension_id,
           student_number = student_school_id, 
           student_first, 
           student_last, 
           school_name, 
           actions, 
           penalties, 
           reported_details, 
           admin_summary, 
           category, 
           grade_level_short, 
           infraction,
           issue_ts) %>%
    #filter(issue_ts_date >= "2017-08-21 00:00") %>%
    collect(n = Inf) %>%
    janitor::clean_names("old_janitor") 
  
  cat("Extracting dates from issue_ts")

  issue_date_ts <- susps_raw %>%
    pull(issue_ts) %>%
    map_df(jsonlite::fromJSON) %>%
    pull(date) %>%
    ymd_hms(tz = "America/Chicago")

  susps <- susps_raw %>%
    mutate(date = issue_date_ts)
    
  cat("Get Membership")
  ps_md <-get_powerschool('ps_membership_reg') 
  
  membership <- ps_md %>%
    filter(calendardate >= "2017-08-19") %>%
    group_by(schoolid, calendardate) %>%
    summarize(N = n()) 
  
  cat("Calcualte ADM")
  adm <- membership %>%
    collect() %>%
    mutate(SY = sprintf("SY%s", 
                        calc_academic_year(calendardate, date_parser = lubridate::ymd, format = 'short'))) %>%
    group_by(schoolid, SY) %>%
    summarize(adm = round(mean(N), 0))
  
  schools <- tibble::tribble(
    ~schoolid, ~school_name, ~combined_name, ~school_full_name,
    78102, "KAP", "Ascend", "KIPP Ascend Primary",
    7810,  "KAMS", "Ascend", "KIPP Ascend Middle",
    400146, "KAC", "Academy", "KIPP Academy Chicago",
    4001632, "KBP", "Bloom", "KIPP Bloom Primary",
    400163, "KBCP", "Bloom", "KIPP Bloom College Prep",
    4001802, "KOP", "One", "KIPP One Primary",
    400180, "KOA", "One", "KIPP One Academy"
  ) 
  
  
  
  adm <- adm %>% inner_join(schools, by="schoolid")
  
  
  
  cat("Extracting penalites nested field")
  penalties <- 
    susps$penalties %>% purrr::map_df(~jsonlite::fromJSON(.x)) %>%
    clean_names("old_janitor") %>%
    select(suspensionid, 
           startdate, 
           enddate, 
           numdays,
           penaltyname 
    ) %>%
    mutate(startdate = ymd(startdate),
           enddate = ymd(enddate),
           diff_days = enddate - startdate,
           numdays = as.integer(numdays)) %>%
    arrange(startdate) %>%
    #filter(!is.na(startdate)) %>%
    mutate(suspensionid = as.integer(suspensionid))
  
  cat('Filtering to OSSs')
  oss <- susps %>%
    inner_join(penalties %>% 
                 filter(str_detect(penaltyname, "Out of School Suspension")),
               by = c("suspension_id" = "suspensionid")) 
  
  
  cat('Calculating OSS Rates')
  
  prep_susps <- . %>%  
    mutate(issue_ts_date = issue_ts %>% map_df(jsonlite::fromJSON) %>% pull(date)) %>%
    mutate(startdate = if_else(is.na(startdate), 
                               ymd_hms(issue_ts_date) +days(1), 
                               ymd_hms(sprintf("%s 00:00:00", startdate)))) %>%
    arrange(startdate) %>%
    mutate(SY = sprintf("SY%s", 
                        calc_academic_year(startdate, date_parser = lubridate::ymd_hms, format = 'short'))) %>%
    #filter(startdate >= ymd("2017-08-24")) %>%
    mutate(month_1 = month(startdate, label = TRUE, abbr = TRUE),
           month = forcats::fct_inorder(as.character(month_1), ordered = TRUE)) %>%
    select(student_number,  
           student_first,
           student_last,
           school_name,
           month, 
           startdate, 
           infraction, 
           category,
           reported_details,
           grade_level_short, 
           numdays, 
           admin_summary,
           SY) %>%
    distinct()
  
  
  calc_rates <- . %>%
    mutate(month_year = floor_date(startdate, unit = "month")) %>%
    group_by(SY, school_name, month, month_year) %>%
    summarize(N_susps = n()) %>%
    group_by(SY, school_name) %>%
    mutate(cum_susps = cumsum(N_susps)
    ) %>%
    inner_join(adm, by = c("SY", "school_name")) %>%
    mutate(susp_rate = N_susps/adm*100,
           cum_susp_rate = cum_susps/adm*100)
  
  
  
  oss_2 <- oss %>% prep_susps
  
  
  
  oss_rates<- oss_2 %>% calc_rates
  
  cat('Filtering and calculating ISSs')
  iss <- susps %>%
    mutate(issue_ts_date = issue_ts %>% map_df(jsonlite::fromJSON) %>% pull(date)) %>%
    inner_join(penalties %>% 
                 filter(penaltyname == "In School Suspension"),
               by = c("suspension_id" = "suspensionid")) #%>%
    #mutate(SY = sprintf("SY%s", 
    #                    calc_academic_year(issue_ts_date, date_parser = lubridate::ymd_hms, format = 'short'))) 
  
  
  iss_2<-iss %>%  prep_susps
  
  iss_rates<- iss_2 %>% calc_rates()
  
  cat("Calculating regional rates")
  oss_w_kop<-oss_2 %>%
    calc_rates() %>%
    mutate(month =  as.character(month),
           month_year = as.Date(month_year)) %>%
    arrange(month_year) %>%
    mutate(month = forcats::fct_inorder(month,ordered = T ))
  
  
  oss_max<-oss_w_kop %>%
    group_by(SY, school_name) %>%
    filter(month_year == max(month_year)) 
  
  
  
  oss_kcs<-oss_2 %>%
    calc_rates() %>%
    mutate(month =  as.character(month),
           month_year = as.Date(month_year)) %>%
    arrange(month_year) %>% 
    mutate(month = forcats::fct_inorder(month, ordered = TRUE)) %>%
    #group_by(SY) %>%
    
    #filter(month == max(month)) %>%
    dplyr::group_by(SY, month) %>%
    dplyr::summarize(cum_susps = sum(cum_susps),
              adm = sum(adm)) %>%
    dplyr::mutate(cum_susp_rate = cum_susps/adm*100) %>%
    dplyr::mutate(school_name = "Region\n(All grades)") %>%
    arrange(SY, month)
    
  
  
  oss_kcs_no_k2 <- oss_2 %>%
    filter(!grade_level_short %in% c("K", "1st", "2nd")) %>%
    calc_rates() %>%
    mutate(month =  as.character(month),
           month_year = as.Date(month_year)) %>%
    arrange(month_year) %>% ungroup() %>%
    mutate(month = forcats::fct_inorder(month,ordered = T )) %>%
    filter(month == max(month)) %>%
    ungroup() %>%
    summarize(cum_susps = sum(cum_susps),
              adm = sum(adm),
              cum_susp_rate = cum_susps/adm*100) %>%
    mutate(school_name = "Region\n(3-8)")
  
  
  
  oss_regional<-bind_rows(oss_max, oss_kcs, oss_kcs_no_k2) %>%
    mutate(regional = grepl("Region", school_name))
  
  
  
  
  oss <- oss_2 
  iss <- iss_2 
  
  cat("Saving to GCS")
  gcs_global_bucket("idea_deanslist")
  
  gcs_results <- gcs_save(susps,
                          penalties,
                          oss,
                          oss_rates,
                          oss_regional,
                          iss,
                          iss_rates,
                          adm,
                          file = "dl_suspensions.Rda")
  
  res$status <- 200
  
}


