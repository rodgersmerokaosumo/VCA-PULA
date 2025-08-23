-- DQC SQL Queries for VCA-PULA Survey
-- Each query extracts data for specific questions from the CSV

-- Question 1: What is the type of the VCA?
-- Valid values: Individual, Registered Company, Cooperative
-- QUERY_Q1_START
SELECT 
    ROW_NUMBER() OVER (ORDER BY response_id) as row_number,
    response_id,
    responses_parsed->>'q_type_of_vca' as vca_type
FROM common_questionnaireresponse
WHERE project_id = 19023;
-- QUERY_Q1_END