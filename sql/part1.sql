create schema BRONZE;

CREATE  TABLE bronze.airbnb_raw(
  listing_id                BIGINT,
  scrape_id                 BIGINT,
  scraped_date              DATE,
  host_id                   BIGINT,
  host_name                 TEXT,
  host_since                DATE,
  host_is_superhost         TEXT,   -- 't'/'f' in raw
  host_neighbourhood        TEXT,
  listing_neighbourhood     TEXT,
  property_type             TEXT,
  room_type                 TEXT,
  accommodates              INT,
  price                     TEXT,   -- keep TEXT in Bronze (may contain $ or ,)
  has_availability          TEXT,   -- 't'/'f'
  availability_30           INT,
  number_of_reviews         INT,
  review_scores_rating      NUMERIC(5,2),
  review_scores_accuracy    NUMERIC(5,2),
  review_scores_cleanliness NUMERIC(5,2),
  review_scores_checkin     NUMERIC(5,2),
  review_scores_communication NUMERIC(5,2),
  review_scores_value       NUMERIC(5,2)
);

ALTER TABLE bronze.airbnb_raw
  ADD CONSTRAINT raw_airbnb_uq_listing_date UNIQUE (listing_id, scraped_date);







CREATE TABLE bronze.census_g01 (
  lga_code_2016 TEXT,
  tot_p_m INTEGER, tot_p_f INTEGER, tot_p_p INTEGER,
  age_0_4_yr_m INTEGER,  age_0_4_yr_f INTEGER,  age_0_4_yr_p INTEGER,
  age_5_14_yr_m INTEGER, age_5_14_yr_f INTEGER, age_5_14_yr_p INTEGER,
  age_15_19_yr_m INTEGER, age_15_19_yr_f INTEGER, age_15_19_yr_p INTEGER,
  age_20_24_yr_m INTEGER, age_20_24_yr_f INTEGER, age_20_24_yr_p INTEGER,
  age_25_34_yr_m INTEGER, age_25_34_yr_f INTEGER, age_25_34_yr_p INTEGER,
  age_35_44_yr_m INTEGER, age_35_44_yr_f INTEGER, age_35_44_yr_p INTEGER,
  age_45_54_yr_m INTEGER, age_45_54_yr_f INTEGER, age_45_54_yr_p INTEGER,
  age_55_64_yr_m INTEGER, age_55_64_yr_f INTEGER, age_55_64_yr_p INTEGER,
  age_65_74_yr_m INTEGER, age_65_74_yr_f INTEGER, age_65_74_yr_p INTEGER,
  age_75_84_yr_m INTEGER, age_75_84_yr_f INTEGER, age_75_84_yr_p INTEGER,
  age_85ov_m INTEGER,     age_85ov_f INTEGER,     age_85ov_p INTEGER,
  counted_census_night_home_m INTEGER,
  counted_census_night_home_f INTEGER,
  counted_census_night_home_p INTEGER,
  count_census_nt_ewhere_aust_m INTEGER,
  count_census_nt_ewhere_aust_f INTEGER,
  count_census_nt_ewhere_aust_p INTEGER,
  indigenous_psns_aboriginal_m INTEGER,
  indigenous_psns_aboriginal_f INTEGER,
  indigenous_psns_aboriginal_p INTEGER,
  indig_psns_torres_strait_is_m INTEGER,
  indig_psns_torres_strait_is_f INTEGER,
  indig_psns_torres_strait_is_p INTEGER,
  indig_bth_abor_torres_st_is_m INTEGER,
  indig_bth_abor_torres_st_is_f INTEGER,
  indig_bth_abor_torres_st_is_p INTEGER,
  indigenous_p_tot_m INTEGER,
  indigenous_p_tot_f INTEGER,
  indigenous_p_tot_p INTEGER,
  birthplace_australia_m INTEGER,
  birthplace_australia_f INTEGER,
  birthplace_australia_p INTEGER,
  birthplace_elsewhere_m INTEGER,
  birthplace_elsewhere_f INTEGER,
  birthplace_elsewhere_p INTEGER,
  lang_spoken_home_eng_only_m INTEGER,
  lang_spoken_home_eng_only_f INTEGER,
  lang_spoken_home_eng_only_p INTEGER,
  lang_spoken_home_oth_lang_m INTEGER,
  lang_spoken_home_oth_lang_f INTEGER,
  lang_spoken_home_oth_lang_p INTEGER,
  australian_citizen_m INTEGER,
  australian_citizen_f INTEGER,
  australian_citizen_p INTEGER,
  age_psns_att_educ_inst_0_4_m INTEGER,
  age_psns_att_educ_inst_0_4_f INTEGER,
  age_psns_att_educ_inst_0_4_p INTEGER,
  age_psns_att_educ_inst_5_14_m INTEGER,
  age_psns_att_educ_inst_5_14_f INTEGER,
  age_psns_att_educ_inst_5_14_p INTEGER,
  age_psns_att_edu_inst_15_19_m INTEGER,
  age_psns_att_edu_inst_15_19_f INTEGER,
  age_psns_att_edu_inst_15_19_p INTEGER,
  age_psns_att_edu_inst_20_24_m INTEGER,
  age_psns_att_edu_inst_20_24_f INTEGER,
  age_psns_att_edu_inst_20_24_p INTEGER,
  age_psns_att_edu_inst_25_ov_m INTEGER,
  age_psns_att_edu_inst_25_ov_f INTEGER,
  age_psns_att_edu_inst_25_ov_p INTEGER,
  high_yr_schl_comp_yr_12_eq_m INTEGER,
  high_yr_schl_comp_yr_12_eq_f INTEGER,
  high_yr_schl_comp_yr_12_eq_p INTEGER,
  high_yr_schl_comp_yr_11_eq_m INTEGER,
  high_yr_schl_comp_yr_11_eq_f INTEGER,
  high_yr_schl_comp_yr_11_eq_p INTEGER,
  high_yr_schl_comp_yr_10_eq_m INTEGER,
  high_yr_schl_comp_yr_10_eq_f INTEGER,
  high_yr_schl_comp_yr_10_eq_p INTEGER,
  high_yr_schl_comp_yr_9_eq_m INTEGER,
  high_yr_schl_comp_yr_9_eq_f INTEGER,
  high_yr_schl_comp_yr_9_eq_p INTEGER,
  high_yr_schl_comp_yr_8_belw_m INTEGER,
  high_yr_schl_comp_yr_8_belw_f INTEGER,
  high_yr_schl_comp_yr_8_belw_p INTEGER,
  high_yr_schl_comp_d_n_g_sch_m INTEGER,
  high_yr_schl_comp_d_n_g_sch_f INTEGER,
  high_yr_schl_comp_d_n_g_sch_p INTEGER,
  count_psns_occ_priv_dwgs_m INTEGER,
  count_psns_occ_priv_dwgs_f INTEGER,
  count_psns_occ_priv_dwgs_p INTEGER,
  count_persons_other_dwgs_m INTEGER,
  count_persons_other_dwgs_f INTEGER,
  count_persons_other_dwgs_p INTEGER
);




create TABLE bronze.census_g02(
  lga_code_2016 TEXT,
  median_age_persons INTEGER,
  median_mortgage_repay_monthly INTEGER,
  median_tot_prsnl_inc_weekly INTEGER,
  median_rent_weekly INTEGER,
  median_tot_fam_inc_weekly INTEGER,
  average_num_psns_per_bedroom NUMERIC(6,3),
  median_tot_hhd_inc_weekly INTEGER,
  average_household_size NUMERIC(6,3)
);


CREATE  TABLE bronze.nsw_lga_suburb (
  lga_name     TEXT,
  suburb_name  TEXT
);


CREATE  table bronze.nsw_lga_code(
  lga_code   TEXT,
  lga_name   TEXT
);



