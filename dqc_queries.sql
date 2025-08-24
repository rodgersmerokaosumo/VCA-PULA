-- DQC SQL Queries for VCA-PULA Survey
-- Each query extracts data for specific questions from the CSV

-- Questions 1-12: VCA Details Section
-- Q1 Valid values: Individual, Registered Company, Cooperative
-- Q2 Valid values: Owner, Manager
-- Q3: VCA full name (text)
-- Q4: VCA age (numeric 18-99)
-- Q5: VCA gender (Male/Female)
-- Q6: VCA phone number
-- Q7: VCA email (optional)
-- Q8: Does VCA have National ID (Yes/No)
-- Q9: National ID number (conditional on Q8)
-- Q10: Photo of National ID (conditional on Q8)
-- Q11: Is VCA legally registered (Yes/No)
-- Q12: TIN number (conditional on Q11)
-- QUERY_Q1_Q12_START
SELECT 
    ROW_NUMBER() OVER (ORDER BY response_id) as row_number,
    response_id,
    responses_parsed->>'q_type_of_vca' as vca_type,
    responses_parsed->>'q_vca_position' as vca_position,
    responses_parsed->'q_candidate_info'->>'name' as vca_full_name,
    responses_parsed->>'q_vca_age' as vca_age,
    responses_parsed->>'q_vca_gender' as vca_gender,
    responses_parsed->'q_candidate_info'->>'phone' as vca_phone_number,
    responses_parsed->>'q_vca_email_address' as vca_email,
    responses_parsed->>'q_vca_id_number_available' as has_national_id,
    responses_parsed->>'q_vca_id_number' as national_id_number,
    responses_parsed->>'q_photo_id_card' as photo_id_card,
    responses_parsed->>'q_legally_registered' as legally_registered,
    responses_parsed->>'q_tin_number' as tin_number
FROM common_questionnaireresponse
WHERE project_id = 19023;
-- QUERY_Q1_Q12_END