
-- Question 1: In which themes are the organization more interested?
SELECT
    dt.theme_name,
    COUNT(fs.answer_id) AS number_of_answers
FROM
    fact_survey fs
JOIN
    dim_Theme dt ON fs.theme_id = dt.theme_id
JOIN
    dim_Question dq ON fs.question_id = dq.question_id
GROUP BY
    dt.theme_name
ORDER BY
    number_of_answers DESC;


-- Question 2: Does the country affect the number of answers?
SELECT
    dc.country_name,
    SUM(fs.number_of_questions_answered) AS total_answers
FROM
    fact_survey fs
JOIN
    dim_Country dc ON fs.country_id = dc.country_id
GROUP BY
    dc.country_name
ORDER BY
    total_answers DESC;


-- Question 3: Does the type of question affect the number of answers?
SELECT
    dq.question_type_id, dqt.question_type_name,
    SUM(fs.number_of_questions_answered) AS total_answers
FROM
    fact_survey fs
JOIN
    dim_Question dq ON fs.question_id = dq.question_id
JOIN public.dim_question_type dqt on dq.question_type_id = dqt.question_type_id
GROUP BY
    dq.question_type_id, dqt.question_type_name
ORDER BY
    total_answers DESC;


-- Question 4: What organizations get the most questions?
SELECT
    dor.organization_name,
    COUNT(fs.question_id) AS total_questions
FROM
    fact_survey fs
JOIN
    dim_Organization dor ON fs.organization_id = dor.organization_id
GROUP BY
    dor.organization_name
ORDER BY
    total_questions DESC;


-- Question 5: What organizations get the most answers?
SELECT
    dog.organization_name,
    SUM(fs.number_of_questions_answered) AS total_answers
FROM
    fact_survey fs
JOIN
    dim_Organization dog ON fs.organization_id = dog.organization_id
GROUP BY
    dog.organization_name
ORDER BY
    total_answers DESC;


-- Question 6: Is the tool used more on a specific season, date?
-- Usage by season
SELECT
    dd.season,
    SUM(fs.number_of_questions_answered) AS total_answers
FROM
    fact_survey fs
JOIN
    dim_Date dd ON fs.date_id = dd.date_id
GROUP BY
    dd.season
ORDER BY
    total_answers DESC;

-- Usage by date
SELECT
    dd.date,
    SUM(fs.number_of_questions_answered) AS total_answers
FROM
    fact_survey fs
JOIN
    dim_Date dd ON fs.date_id = dd.date_id
GROUP BY
    dd.date
ORDER BY
    total_answers DESC;



--
SELECT
    dg.organization_name,
    SUM(fs.number_of_questions_answered) AS total_answers
FROM
    fact_survey fs
JOIN
    dim_Organization dg ON fs.organization_id = dg.organization_id
GROUP BY
    dg.organization_name
ORDER BY
    total_answers DESC
LIMIT 1;


