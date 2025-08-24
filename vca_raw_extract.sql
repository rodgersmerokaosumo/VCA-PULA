
/* VCA Survey — RAW extract (no formatting/restructuring)
   - Table: common_questionnaireresponse
   - PK: cce_id
   - JSONB: responses (rsp), farmer_responses (frp)
   - Photos (q_photo_id_card, q_vca_photo_sign_post, q_vca_photo_premises) NOT selected
*/

WITH src AS (
  SELECT
    res.cce_id,                       
    res.response_id,
    res.project_id,
    res.questionnaire_id,
    res.questionnaire_id_text,
    res.uai_id,
    res.adm_2_id,
    res.submitted_by_id,
    res.user_id,
    res.start_time,
    res.end_time,
    res.created,
    res.modified,
    res.date_modified,
    res.is_test,
    res.farm_id,
    res.responses::jsonb            AS rsp,
    res.farmer_responses::jsonb     AS frp
  FROM common_questionnaireresponse res
  WHERE res.project_id = 19023
)

SELECT
  /* -------- Meta -------- */
  cce_id, response_id, project_id, questionnaire_id, questionnaire_id_text,
  uai_id, adm_2_id, submitted_by_id, user_id,
  created, modified, date_modified, start_time, end_time, is_test, farm_id,

  /* -------- Top-level JSON objects -------- */
  rsp->'metadata'                                  AS metadata_json,
  rsp->'q_vca_gps'                                 AS q28_vca_gps_json,

  /* -------- Identity / contact (both sources kept raw) -------- */
  rsp->>'q_type_of_vca'                            AS q_type_of_vca,
  rsp->>'q_vca_position'                           AS q_vca_position,
  rsp->>'q_vca_gender'                             AS q_vca_gender,
  rsp->>'q_vca_email_address'                      AS q_vca_email_address,
  rsp->>'q_vca_id_number_available'                AS q_vca_id_number_available,
  rsp->>'q_vca_id_number'                          AS q_vca_id_number,
  rsp->'q_candidate_info'                          AS q_candidate_info_json,
  rsp->'q_candidate_info'->>'name'                 AS q_candidate_name,
  rsp->'q_candidate_info'->>'phone'                AS q_candidate_phone,
  rsp->'q_candidate_info'->>'id_number'            AS q_candidate_id_number,
  /* farmer_responses (if your form also writes there) */
  frp->>'name'                                     AS fr_name,
  frp->>'age'                                      AS fr_age,
  frp->>'gender'                                   AS fr_gender,
  frp->>'phone-number'                             AS fr_phone_number,
  frp->>'id_number'                                AS fr_id_number,

  /* -------- Registration & TIN -------- */
  rsp->>'q_legally_registered'                     AS q_legally_registered,
  rsp->>'q_tin_number'                             AS q_tin_number,

  /* -------- Business category flags (as stored) -------- */
  rsp->>'q_vca_mill'                               AS q_vca_mill,
  rsp->>'q_vca_shop'                               AS q_vca_shop,
  rsp->>'q_vca_other'                              AS q_vca_other,
  rsp->>'q_vca_store'                              AS q_vca_store,
  rsp->>'q_vca_trader'                             AS q_vca_trader,
  rsp->>'q_vca_roaster'                            AS q_vca_roaster,
  rsp->>'q_vca_exporter'                           AS q_vca_exporter,
  rsp->>'q_vca_extractor'                          AS q_vca_extractor,
  rsp->>'q_vca_warehouse'                          AS q_vca_warehouse,
  rsp->>'q_vca_hulling_station'                    AS q_vca_hulling_station,
  rsp->>'q_vca_grading_facility'                   AS q_vca_grading_facility,
  rsp->>'q_other_business_category'                AS q_other_business_category,

  /* -------- General-level answers -------- */
  rsp->'q_type_coffee_sourced'                     AS q_type_coffee_sourced_json,
  rsp->'q_coffee_form'                             AS q_coffee_form_json,
  rsp->>'q_district_coffee_received'               AS q_district_coffee_received,
  rsp->>'q_coffee_recieved_in_a_year_kgs'          AS q_coffee_recieved_in_a_year_kgs,
  rsp->'q_recieve_coffee_from'                     AS q_recieve_coffee_from_json,
  rsp->>'q_huller_operated'                        AS q_huller_operated,

  /* -------- Per-category: business names -------- */
  rsp->>'q_gf_business_name'                       AS q_gf_business_name,
  rsp->>'q_hs_business_name'                       AS q_hs_business_name,
  rsp->>'q_wh_business_name'                       AS q_wh_business_name,
  rsp->>'q_mill_business_name'                     AS q_mill_business_name,
  rsp->>'q_shop_business_name'                     AS q_shop_business_name,
  rsp->>'q_store_business_name'                    AS q_store_business_name,
  rsp->>'q_trader_business_name'                   AS q_trader_business_name,
  rsp->>'q_roaster_business_name'                  AS q_roaster_business_name,
  rsp->>'q_exporter_business_name'                 AS q_exporter_business_name,
  rsp->>'q_extractor_business_name'                AS q_extractor_business_name,
  rsp->>'q_other_business_name'                    AS q_other_business_name,

  /* -------- Per-category: business addresses (typo retained: bussines) -------- */
  rsp->>'q_gf_bussines_address'                    AS q_gf_bussines_address,
  rsp->>'q_hs_bussines_address'                    AS q_hs_bussines_address,
  rsp->>'q_wh_bussines_address'                    AS q_wh_bussines_address,
  rsp->>'q_mill_bussines_address'                  AS q_mill_bussines_address,
  rsp->>'q_shop_bussines_address'                  AS q_shop_bussines_address,
  rsp->>'q_store_bussines_address'                 AS q_store_bussines_address,
  rsp->>'q_trader_bussines_address'                AS q_trader_bussines_address,
  rsp->>'q_roaster_bussines_address'               AS q_roaster_bussines_address,
  rsp->>'q_exporter_bussines_address'              AS q_exporter_bussines_address,
  rsp->>'q_extractor_bussines_address'             AS q_extractor_bussines_address,
  rsp->>'q_other_bussines_address'                 AS q_other_bussines_address,

  /* -------- Per-category: max operating capacity -------- */
  rsp->>'q_gf_max_operating_capacity'              AS q_gf_max_operating_capacity,
  rsp->>'q_hs_max_operating_capacity'              AS q_hs_max_operating_capacity,
  rsp->>'q_wh_max_operating_capacity'              AS q_wh_max_operating_capacity,
  rsp->>'q_mill_max_operating_capacity'            AS q_mill_max_operating_capacity,
  rsp->>'q_shop_max_operating_capacity'            AS q_shop_max_operating_capacity,
  rsp->>'q_other_max_operating_capacity'           AS q_other_max_operating_capacity,
  rsp->>'q_store_max_operating_capacity'           AS q_store_max_operating_capacity,
  rsp->>'q_trader_max_operating_capacity'          AS q_trader_max_operating_capacity,
  rsp->>'q_roaster_max_operating_capacity'         AS q_roaster_max_operating_capacity,
  rsp->>'q_exporter_max_operating_capacity'        AS q_exporter_max_operating_capacity,
  rsp->>'q_extractor_max_operating_capacity'       AS q_extractor_max_operating_capacity,

  /* -------- Per-category: max storage -------- */
  rsp->>'q_gf_max_storage'                         AS q_gf_max_storage,
  rsp->>'q_hs_max_storage'                         AS q_hs_max_storage,
  rsp->>'q_wh_max_storage'                         AS q_wh_max_storage,
  rsp->>'q_mill_max_storage'                       AS q_mill_max_storage,
  rsp->>'q_shop_max_storage'                       AS q_shop_max_storage,
  rsp->>'q_other_max_storage'                      AS q_other_max_storage,
  rsp->>'q_store_max_storage'                      AS q_store_max_storage,
  rsp->>'q_trader_max_storage'                     AS q_trader_max_storage,
  rsp->>'q_roaster_max_storage'                    AS q_roaster_max_storage,
  rsp->>'q_exporter_max_storage'                   AS q_exporter_max_storage,
  rsp->>'q_extractor_max_storage'                  AS q_extractor_max_storage,

  /* -------- Per-category: total processing (only HS provided) -------- */
  rsp->>'q_total_processing_per_year_hs'           AS q_total_processing_per_year_hs,

  /* -------- Per-category: coffee form (arrays kept raw JSON) -------- */
  rsp->'q_coffee_form_gf'                          AS q_coffee_form_gf_json,
  rsp->'q_coffee_form_hs'                          AS q_coffee_form_hs_json,
  rsp->'q_coffee_form_wh'                          AS q_coffee_form_wh_json,
  rsp->'q_coffee_form_mill'                        AS q_coffee_form_mill_json,
  rsp->'q_coffee_form_shop'                        AS q_coffee_form_shop_json,
  rsp->'q_coffee_form_store'                       AS q_coffee_form_store_json,
  rsp->'q_coffee_form_trader'                      AS q_coffee_form_trader_json,
  rsp->'q_coffee_form_roaster'                     AS q_coffee_form_roaster_json,
  rsp->'q_coffee_form_exporter'                    AS q_coffee_form_exporter_json,
  rsp->'q_coffee_form_extractor'                   AS q_coffee_form_extractor_json,

  /* -------- Per-category: type coffee sourced (arrays kept raw JSON) -------- */
  rsp->'q_type_coffee_sourced_gf'                  AS q_type_coffee_sourced_gf_json,
  rsp->'q_type_coffee_sourced_hs'                  AS q_type_coffee_sourced_hs_json,
  rsp->'q_type_coffee_sourced_wh'                  AS q_type_coffee_sourced_wh_json,
  rsp->'q_type_coffee_sourced_mill'                AS q_type_coffee_sourced_mill_json,
  rsp->'q_type_coffee_sourced_shop'                AS q_type_coffee_sourced_shop_json,
  rsp->'q_type_coffee_sourced_store'               AS q_type_coffee_sourced_store_json,
  rsp->'q_type_coffee_sourced_trader'              AS q_type_coffee_sourced_trader_json,
  rsp->'q_type_coffee_sourced_roaster'             AS q_type_coffee_sourced_roaster_json,
  rsp->'q_type_coffee_sourced_exporter'            AS q_type_coffee_sourced_exporter_json,
  rsp->'q_type_coffee_sourced_extractor'           AS q_type_coffee_sourced_extractor_json,

  /* -------- Per-category: districts -------- */
  rsp->>'q_district_coffee_received_gf'            AS q_district_coffee_received_gf,
  rsp->>'q_district_coffee_received_hs'            AS q_district_coffee_received_hs,
  rsp->>'q_district_coffee_received_wh'            AS q_district_coffee_received_wh,
  rsp->>'q_district_coffee_received_mill'          AS q_district_coffee_received_mill,
  rsp->>'q_district_coffee_received_shop'          AS q_district_coffee_received_shop,
  rsp->>'q_district_coffee_received_store'         AS q_district_coffee_received_store,
  rsp->>'q_district_coffee_received_trader'        AS q_district_coffee_received_trader,
  rsp->>'q_district_coffee_received_roaster'       AS q_district_coffee_received_roaster,
  rsp->>'q_district_coffee_received_exporter'      AS q_district_coffee_received_exporter,
  rsp->>'q_district_coffee_received_extractor'     AS q_district_coffee_received_extractor,

  /* -------- Per-category: who you receive from (arrays kept raw JSON) -------- */
  rsp->'q_recieve_coffee_from_gf'                  AS q_recieve_coffee_from_gf_json,
  rsp->'q_recieve_coffee_from_hs'                  AS q_recieve_coffee_from_hs_json,
  rsp->'q_recieve_coffee_from_wh'                  AS q_recieve_coffee_from_wh_json,
  rsp->'q_recieve_coffee_from_mill'                AS q_recieve_coffee_from_mill_json,
  rsp->'q_recieve_coffee_from_shop'                AS q_recieve_coffee_from_shop_json,
  rsp->'q_recieve_coffee_from_store'               AS q_recieve_coffee_from_store_json,
  rsp->'q_recieve_coffee_from_trader'              AS q_recieve_coffee_from_trader_json,
  rsp->'q_recieve_coffee_from_roaster'             AS q_recieve_coffee_from_roaster_json,
  rsp->'q_recieve_coffee_from_exporter'            AS q_recieve_coffee_from_exporter_json,
  rsp->'q_recieve_coffee_from_extractor'           AS q_recieve_coffee_from_extractor_json,
  rsp->>'q_recieve_coffee_from_others_trader'      AS q_recieve_coffee_from_others_trader,

  /* -------- Per-category: annual kgs -------- */
  rsp->>'q_coffee_recieved_in_a_year_kgs_gf'       AS q_coffee_recieved_in_a_year_kgs_gf,
  rsp->>'q_coffee_recieved_in_a_year_kgs_hs'       AS q_coffee_recieved_in_a_year_kgs_hs,
  rsp->>'q_coffee_recieved_in_a_year_kgs_wh'       AS q_coffee_recieved_in_a_year_kgs_wh,
  rsp->>'q_coffee_recieved_in_a_year_kgs_mill'     AS q_coffee_recieved_in_a_year_kgs_mill,
  rsp->>'q_coffee_recieved_in_a_year_kgs_shop'     AS q_coffee_recieved_in_a_year_kgs_shop,
  rsp->>'q_coffee_recieved_in_a_year_kgs_store'    AS q_coffee_recieved_in_a_year_kgs_store,
  rsp->>'q_coffee_recieved_in_a_year_kgs_trader'   AS q_coffee_recieved_in_a_year_kgs_trader,
  rsp->>'q_coffee_recieved_in_a_year_kgs_roaster'  AS q_coffee_recieved_in_a_year_kgs_roaster,
  rsp->>'q_coffee_recieved_in_a_year_kgs_exporter' AS q_coffee_recieved_in_a_year_kgs_exporter,
  rsp->>'q_coffee_recieved_in_a_year_kgs_extractor'AS q_coffee_recieved_in_a_year_kgs_extractor

FROM src
ORDER BY end_time DESC NULLS LAST, created DESC;
