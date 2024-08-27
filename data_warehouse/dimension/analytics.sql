-- Question 1:In which themes are the youths more interested?
SELECT
    T.theme_name,
    COUNT(S.survey_id) AS total_surveys
FROM
    Fact_Survey S
JOIN
    Dim_Theme T ON S.theme_id = T.theme_id
GROUP BY
    T.theme_name
ORDER BY
    total_surveys DESC;

-- Question 2: What trends can be observed in youth engagement?
SELECT
    D.year,
    D.month,
    COUNT(S.survey_id) AS total_surveys
FROM
    Fact_Survey S
JOIN
    Dim_Date D ON S.date_id = D.date_id
GROUP BY
    D.year, D.month
ORDER BY
    D.year, D.month;


-- Question 3: Does the distribution of question types affect youth engagement?

SELECT
    QT.question_type_name,
    COUNT(SQ.question_id) AS total_questions
FROM
    survey_question_bridge SQ
JOIN
    Dim_Question Q ON SQ.question_id = Q.question_id
JOIN
    Dim_Question_Type QT ON Q.question_type_id = QT.question_type_id
GROUP BY
    QT.question_type_name
ORDER BY
    total_questions DESC;


-- Question 4: Which themes are most popular during specific seasons?

SELECT
    D.season,
    D.weekday,
    T.theme_name,
    COUNT(S.survey_id) AS total_surveys
FROM
    Fact_Survey S
JOIN
    Dim_Date D ON S.date_id = D.date_id
JOIN
    Dim_Theme T ON S.theme_id = T.theme_id
GROUP BY
    D.season, D.weekday, T.theme_name
ORDER BY
    D.season, total_surveys DESC;


-- Question 5: What organizations get the most questions?

SELECT
    O.organization_name,
    COUNT(SQ.question_id) AS total_questions
FROM
    survey_question_bridge SQ
JOIN
    Fact_Survey S ON SQ.survey_id = S.survey_id
JOIN
    Dim_Organization O ON S.organization_id = O.organization_id
GROUP BY
    O.organization_name
ORDER BY
    total_questions DESC;


-- Question 6: What subthemes interest the youth the most?

SELECT
    ST.subtheme_name,
    COUNT(SQ.question_id) AS total_questions
FROM
    survey_question_bridge SQ
JOIN
    Fact_Survey S ON SQ.survey_id = S.survey_id
JOIN
    Dim_SubTheme ST ON S.subtheme_id = ST.subtheme_id
GROUP BY
    ST.subtheme_name
ORDER BY
    total_questions DESC;



--- Question 7: What country has the most youth engagement?
SELECT
    C.country_name,
    COUNT(SC.survey_id) AS total_surveys
FROM
    survey_country_bridge SC
JOIN
    Dim_Country C ON SC.country_id = C.country_id
GROUP BY
    C.country_name
ORDER BY
    total_surveys DESC;


--- Question 8: Trending subthemes this year?

SELECT
    ST.subtheme_name,
    EXTRACT(YEAR FROM CURRENT_DATE) AS year,  -- Show the current year
    COUNT(SQ.question_id) AS total_questions
FROM
    survey_question_bridge SQ
JOIN
    Fact_Survey S ON SQ.survey_id = S.survey_id
JOIN
    Dim_SubTheme ST ON S.subtheme_id = ST.subtheme_id
JOIN
    Dim_Date D ON S.date_id = D.date_id
WHERE
    EXTRACT(YEAR FROM CURRENT_DATE) = D.year  -- For the current year
GROUP BY
    ST.subtheme_name, EXTRACT(YEAR FROM CURRENT_DATE)
ORDER BY
    total_questions DESC;


