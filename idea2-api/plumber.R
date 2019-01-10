# This is a simple API to call a script that 
# relies on the mapvizieR R package to prep data
# for KIPP Chicago's IDEA data reporting tool. This 
# endpoint is called by Airflow 

library(plumber)

library(dplyr)
library(lubridate)
library(purrr)
library(stringr)
library(googleCloudStorageR)
library(janitor)
library(forcats)

library(mapvizieR)
library(silounloadr)

library(googlesheets)
library(here)



#* @apiTitle Data Prep Endpoints for IDEA2

gcs_auth_file <- Sys.getenv("GCS_AUTH_FILE")
bq_auth_file <-Sys.getenv("BQ_AUTH_FILE")
bq_project <- Sys.getenv("BQ_PROJECT")

cat(sprintf("BQ_AUTH_FILE is %s", bq_auth_file))

bigrquery::set_service_token(bq_auth_file)

first_day <- Sys.getenv("FIRST_DAY") 

#* Trigger NWEA MAP prep script for IDEA
#* @get /run_idea_map 
function(res){
  
  # Quick function to separate single CDF table into Students and Results
  separate_cdf <- function(combinded_cdf, district_name = "Not provided"){
    ar_names <- names(ex_CombinedAssessmentResults) %>% tolower
    stu_names <- names(ex_CombinedStudentsBySchool) %>% tolower
    
    if (!"districtname" %in% tolower(names(combinded_cdf))) {
      combinded_cdf <- combinded_cdf %>% mutate_(districtname = ~district_name)
    }
    
    roster<-combinded_cdf %>%
      select_(.dots = stu_names) %>%
      unique
    
    cdf<-combinded_cdf %>% select(-studentlastname:-studentfirstname,
                                  -studentmi:-studentgender,
                                  -grade) %>%
      mutate(testid=as.character(testid))
    
    out <- list(cdf = cdf,
                roster = roster)
    
  }
  
  
  
  
  
  cat("Setting variables")
  
  schools <- data_frame(schoolid = c(78102, 7810, 400146, 4001632, 400163, 4001802, 400180),
                        schoolname = c("Ascend Primary", "Ascend Middle", "Academy", "Bloom Primary",  "Bloom", "One Primary", "One Academy"),
                        schoolabbreviation =c("KAP", "KAMS", "KAC", "KBP", "KBCP", "KOP", "KOA"))
  
  first_four_years_ago <- floor_date(ymd(first_day) - years(4), unit = "week") %>%
    as.character() 

  
  cat("Connection to Silo for MAP Data")
  map_cdf <- silounloadr::get_nwea_map('cdf_combined_kipp_cps')
  
  cat("Detecting test term names")
  test_term_names <- map_cdf %>%
    select(term_name, test_start_date) %>%
    filter(test_start_date >= first_four_years_ago) %>%
    select(term_name) %>%
    distinct() %>%
    collect()
  
  cat("Pulling MAP data by term")
  get_map_by_term <- function(termname) {
    map_cdf %>% filter(term_name == termname) %>% collect()
  }
  
  map_cdf_2 <- test_term_names$term_name %>%
    purrr::map_df(~get_map_by_term(.))
  
  cat("Renaming colums (janitor-style)")
  names(map_cdf_2) <- str_replace_all(names(map_cdf_2), "_", "") %>% tolower()
  
  cat("Excluding Survey only and some light munging")
  map_cdf_3 <- map_cdf_2 %>%
    mutate(testtype = if_else(is.na(testtype), "Survey With Goals", testtype),
           testid = as.character(testid)) %>%
    filter(testtype == "Survey With Goals",
           growthmeasureyn == 'TRUE') %>%
    mutate(teststartdate = as.character(ymd(teststartdate)),
           testid = if_else(is.na(testid),
                            paste(studentid, measurementscale, teststartdate, testdurationminutes, sep = "_"),
                            testid),
           schoolname = if_else(str_detect(schoolname, "Create"), 
                                "KIPP Academy Chicago", 
                                schoolname)
    )
  
  
  cat("Separate combined table into assessment results and roster")
  
  map_sep <- separate_cdf(map_cdf_3, district_name = "KIPP Chicago")
  
  cat("Create mapvizieR object for 2015 norms")
  
  map_sep$cdf <- map_sep$cdf %>%
    mutate(goal7name = NA, 
           goal7ritscore = NA, 
           goal7stderr = NA, 
           goal7range = NA, 
           goal7adjective = NA, 
           goal8name = NA, 
           goal8ritscore = NA, 
           goal8stderr = NA, 
           goal8range = NA, 
           goal8adjective = NA, 
           projectedproficiencystudy3 = NA, 
           projectedproficiencylevel3 = NA) %>%
    distinct()
  
  map_sep$roster <- map_sep$roster %>% distinct()
  
  map_mv_15 <-  
    mapvizieR::mapvizieR(
      cdf = map_sep$cdf,
      roster = map_sep$roster,
      include_unsanctioned_windows = TRUE,
      verbose = TRUE
    )
  
  cat("Create summary objects")
  
  map_sum_15 <- summary(map_mv_15$growth_df)
  
  cat("Get current student roster from PowerSchool")
  current_ps <- get_powerschool("students") %>%
    select(studentid = student_number,
           schoolid, 
           grade_level,
           enroll_status) %>%
    filter(enroll_status == 0) %>%
    collect()
  
  names(current_ps) <- tolower(names(current_ps))
  
  cat("calculate students per grade")
  student_enrollment <- current_ps %>%
    group_by(schoolid, grade_level) %>%
    summarize(N = n()) %>%
    inner_join(schools, by = "schoolid") %>%
    rename(grade = grade_level)
  
  cat("Calculate current students tested")
  current_map_term <- map_mv_15$cdf %>%
    ungroup() %>%
    filter(teststartdate == max(teststartdate)) %>%
    select(termname) %>%
    unique() %>%
    .[[1]]
  
  tested <- map_mv_15$cdf %>%
    filter(termname == current_map_term,
           growthmeasureyn) %>%
    group_by(schoolname, grade, measurementscale) %>%
    summarize(n_tested = n()) %>%
    mutate(schoolabbreviation = abbrev(schoolname, list(old = "KAPS", new = "KAP"))) %>%
    ungroup() %>%
    select(schoolabbreviation, grade, measurementscale, n_tested)
  
  student_enrollment_tested <-
    tested %>%
    left_join(student_enrollment,
              by = c("schoolabbreviation", "grade")) %>%
    select(School = schoolabbreviation,
           Grade = grade,
           Subject = measurementscale,
           Enrolled = N,
           Tested = n_tested
    ) %>%
    mutate(Percent = Tested/Enrolled)
  
  cat("Getting historical scores")
  hist_scores <- map_mv_15$cdf %>%
    ungroup() %>% 
    as_tibble() %>% # fixes grouping issue (Groups = [?], which is not the same as non groups)
    inner_join(map_mv_15$roster %>%
                 ungroup() %>%
                 filter(implicit_cohort >= 2023) %>%
                 select(termname, studentid, studentlastname,
                        studentfirstname, implicit_cohort, year_in_district),
               by = c("termname",  "studentid")) %>% 
    inner_join(current_ps %>%
                 select(studentid),
               by = "studentid") %>%
    mutate(SY = sprintf("%s-%s", map_year_academic, map_year_academic + 1),
           School = mapvizieR::abbrev(schoolname, list(old = "KAPS", new = "KAP")),
           tested_at_kipp = as.logical(testedatkipp)) %>% 
    select(SY,
           School,
           Grade = grade,
           Season = fallwinterspring,
           Subject = measurementscale,
           ID = studentid,
           "First Name" = studentfirstname,
           "Last Name" = studentlastname,
           "RIT Score" = testritscore,
           "Percentile" = testpercentile,
           "Date Taken" = teststartdate,
           "Taken at KIPP?" = tested_at_kipp
    ) %>%
    arrange(desc(SY), Season, Subject, School, Grade)
  
  cat("Saving data to GCS.")
  gcs_global_bucket("idea_map")
  
  gcs_results <- gcs_save(#map_mv_15,
    map_sum_15,
    current_ps,
    current_map_term,
    student_enrollment_tested,
    hist_scores,
    file = "map.rda")
  
  
  res$status <- 200
}


#* Trigger Deanslist Supsenions prep script for IDEA
#* @get /run_idea_dl_suspensions
function(res){
  
  cat("Get DL Suspensions")
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
  
  
  iss_2<-iss %>%  prep_susps()
  
  if(min(iss_2$month) == "Sep") {
    iss_2 <- iss_2 %>% mutate(month = fct_shift(month, -1))
  }
             
  
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


#* Trigger Duplicate Grade Review prep script for IDEA
#* @get /run_idea_grade_review
function(res){
  schools <- data_frame(schoolid = c(78102, 7810, 400146, 400163, 4001802, 400180, 4001632),
                        schoolabbreviation =c("KAP", "KAMS", "KAC", "KBCP", "KOP", "KOA", "KBP"))
  
  cat("Get Students Table")
  students <- get_powerschool("students") %>%
    select(studentid = id,
           student_number,
           first_name, 
           last_name,
           grade_level,
           home_room,
           enroll_status,
           schoolid) %>%
    collect()
  
  cat("Get Enrollments Table")
  enrollments <- get_powerschool("ps_enrollment_all") %>% 
    select(studentid,
           schoolid,
           entrydate,
           exitdate,
           exitcode,
           yearid) %>%
    collect()
  
  cat("Calculate First Year and Term ID")
  current_first_year <- calc_academic_year(lubridate::today(), 
                                           format = "first_year") 
  
  ps_termid <- calc_ps_termid(current_first_year)
  
  cat("Idenitfy DNA students")
  dna_students <- enrollments %>% 
    filter(yearid == ps_termid/100) %>%
    mutate(exitdate = as_date(exitdate)) %>%
    group_by(schoolid,
             studentid) %>%
    filter(exitdate == min(exitdate)) %>% 
    mutate(dna = exitdate %in% as_date("2018-08-20") | #Use Sys Env First Day??
             exitcode == 99)
  
  cat("Get CC Table")
  cc_unique <- cc %>%
    filter(termid %in% c(ps_termid, (-1*ps_termid))) %>%
    select(course_number,
           section_number,
           sectionid) %>%
    unique()
  
  cat("Calculate Second Year")
  current_last_year <- calc_academic_year(today(), format = "second_year")
  
  cat("Get Gradebooks Table")
  gradebooks <- get_illuminate("gradebooks", schema = "gradebook") %>% 
    filter(academic_year == current_last_year) %>%
    select(gradebook_id,
           created_by,
           gradebook_name,
           active,
           is_deleted,
           academic_year) %>%
    collect()
  
  cat("Get Illuminate Students Table")
  ill_students <- get_illuminate("students", "public") %>%
    select(student_id,
           local_student_id) %>%
    collect()
  
  cat("Get Gradebook Sections Table")
  gradebook_sections <- get_illuminate("gradebook_section_course_aff", schema = "gradebook") %>%
    select(gradebook_id,
           ill_sec_id = section_id,
           user_id) %>%
    collect()
  
  cat("Get PS Terms Table")
  terms <- get_powerschool("terms") %>%
    filter(id >= ps_termid) %>%
    select(id,
           abbreviation,
           firstday,
           lastday,
           schoolid) %>%
    collect()
  
  cat("Get Illuminate Sections Table")
  illuminate_sec <- get_illuminate("sections",
                                   "public") %>% 
    select(ill_sec_id = section_id,
           ps_sec_id = local_section_id) %>% 
    collect()
  
  cat("Find current SY quarter terms")
  current_q_dates <- terms %>%
    select(-schoolid) %>%
    filter(grepl("Q", abbreviation)) %>%
    mutate(q_number = stringr::str_extract(abbreviation, "\\d")) %>%
    group_by(q_number, 
             firstday, 
             lastday) %>%
    summarise() %>%
    as.data.frame() %>%
    mutate(q_interval = lubridate::interval(firstday, lastday))
  
  q_dates_no_interval <- current_q_dates %>%
    select(-q_interval)
  
  cat("Identify last day of school")
  last_school_day <- q_dates_no_interval %>% 
    dplyr::filter(q_number == 4) %>%
    select(lastday)
  
  cat("Identify first day of school")
  first_school_day <- q_dates_no_interval %>%
    filter(q_number == 1) %>%
    select(firstday)
  
  identify_quarter <- . %>%
    purrr::map(function(x) x %within% current_q_dates$q_interval) %>% 
    purrr::map(function(x) which(x)) %>%
    as.double()
  
  cat("Identify current quarter and interval")
  date_within_quarter <- today() %>%
    identify_quarter()
  
  id_q_dates <- q_dates_no_interval %>%
    filter(q_number %in% c(date_within_quarter - 1, date_within_quarter))
  
  if(date_within_quarter == 1){
    past_q_firstday <- id_q_dates %>%
      select(firstday)
  } else {
    past_q_firstday <- id_q_dates %>%
      filter(q_number %in% c(date_within_quarter -1)) %>%
      select(firstday)  
  }
  
  current_q_lastday <- id_q_dates %>%
    filter(q_number %in% date_within_quarter) %>%
    select(lastday)
  
  cat("Get overall grades")
  overall_grades <- get_illuminate("overall_score_cache", schema = "gradebook") %>%
    filter(timeframe_end_date <= current_q_lastday$lastday,#date_within_quarter$lastday,
           timeframe_start_date >= past_q_firstday$firstday) %>%
    select(gradebook_id,
           calculated_at,
           mark,
           percentage,
           student_id,
           timeframe_end_date,
           timeframe_start_date) %>%
    collect(n= Inf) 
  
  cat("Filter max calculated_at day/time")
  overall_grades_recent <- overall_grades %>%
    group_by(gradebook_id,
             student_id,
             timeframe_start_date,
             timeframe_end_date) %>%
    filter(calculated_at == max(calculated_at))
  
  cat("Get DeansList Rosters")
  file_list <- dir(path = here::here("/DL Rosters"), pattern = "1819", full.names = TRUE)
  
  gradebook_df_list <- file_list %>%
    map(read_csv) %>%
    map(janitor::clean_names) %>%
    map_df(bind_rows) %>%
    mutate(sec_id = secondary_integration_id_at_load)
  
  cat("Combining Gradebooks with sections, students tables")
  grades_gb_name <- overall_grades_recent %>%
    filter(!is.na(mark)) %>%
    left_join(gradebooks,
              by = "gradebook_id") %>%
    left_join(gradebook_df_list %>%
                filter(!is.na(gradebook_name_at_load)) %>%
                select(sec_id,
                       gradebook_name_at_load),
              by = c("gradebook_name" = "gradebook_name_at_load")) %>% 
    left_join(ill_students %>%
                mutate(student_number = as.double(local_student_id)),
              by = "student_id")
  
  grades_sections <- grades_gb_name %>%
    filter(!is_deleted) %>%
    left_join(students,
              by = "student_number") %>%
    filter(grade_level > 3) %>%
    left_join(cc_unique,
              by = c("sec_id" = "sectionid"))
  
  cat("Identify previous and current classes")
  students_sections <- students %>%
    filter(enroll_status == 0,
           grade_level > 3) %>%
    left_join(cc %>%
                select(dateenrolled,
                       dateleft,
                       sectionid,
                       course_number,
                       studentid),
              by = "studentid") %>% 
    filter(dateenrolled >= first_school_day$firstday) %>%
    mutate(status = if_else(sectionid < 0, "Previous Class", "Current Class"))
  
  cat("Combining final gradebook table")
  final_grade_data <- grades_sections %>%
    left_join(schools,
              by = "schoolid") %>%
    left_join(students_sections %>%
                mutate(abs_sec_id = abs(sectionid)) %>%
                select(student_number,
                       abs_sec_id,
                       status,
                       dateleft),
              by = c("student_number",
                     "sec_id" = "abs_sec_id")) %>% 
    left_join(dna_students %>% 
                select(studentid,
                       schoolid,
                       dna,
                       dna_exitdate = exitdate),
              by = c("studentid",
                     "schoolid")) %>%
    #some students came, left, then returned (and rostered to a new HR)
    mutate(status0 = status,
           status = if_else(is.na(status) & dna, "Previous Class", status0)) %>%
    filter(enroll_status == 0,
           active) %>% #filter for active == TRUE grade books
    mutate(dna_exitdate = as_date(dna_exitdate),
           dateleft = as_date(dateleft),
           dateleft = if_else(is.na(dateleft) & dna, dna_exitdate, dateleft),
           q_current = identify_quarter(timeframe_end_date),
           dateleft = as_date(dateleft),
           transfer = dateleft < last_school_day$lastday + days(1), #adding 1 day because Chris' has classes ending on 6/15 but enrollment ends 6/14
           q_transfer = if_else(transfer, identify_quarter(dateleft), 0))
  
  cat("Homeroom table for shiny selection filter")
  homerooms <- final_grade_data %>% #View()
    ungroup() %>% 
    select(schoolid, 
           home_room) %>% 
    unique() %>% #View()
    mutate(grade_level = str_extract(home_room, "[4-8]")) %>%
    left_join(schools,
              by = "schoolid") %>%
    mutate(grade_level = as.integer(grade_level))
}

