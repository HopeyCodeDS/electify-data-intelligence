SELECT
    org.name AS organization_name,
    org.country AS Country,
    org.datecreated AS org_datecreated,
    org.datemodified AS org_datemodified,
    org.status AS org_status,
    qnnr.name AS questionnaire_name,
    qnnr.datecreated AS qnnr_datecreated,
    qnnr.datemodified AS qnnr_datemodified,
    qnnr.theme_id,
    qnnr.type AS questionnaire_type,
    qnnr.status AS questionnaire_status,
    qn.creation_date AS question_creation_date,
    qn.question_content,
    qn.question_type,
    qn.creation_date AS question_creation_date,
    ans.answer_type,
    ans.date AS answer_date
FROM
    organization org
LEFT JOIN
    questionnaire qnnr ON org.id = qnnr.organization_id
LEFT JOIN
    question qn ON qnnr.id = qn.questionnaire_id
LEFT JOIN
    answer ans ON qn.id = ans.question_id
ORDER BY
    org.id, qnnr.id, qn.id;


SELECT
    qn.id AS question_id,
    qnnr.theme_id AS theme_id,
    qn.creation_date AS question_creation_date,
    org.id AS organization_id,
    COUNT(ans.answer_type) AS number_of_questions_answered,
    org.name AS organization_name,
    org.country AS country,
    org.datecreated AS org_datecreated,
    org.datemodified AS org_datemodified,
    org.status AS org_status,
    qnnr.name AS questionnaire_name,
    qnnr.datecreated AS qnnr_datecreated,
    qnnr.datemodified AS qnnr_datemodified,
    qnnr.type AS questionnaire_type,
    qnnr.status AS questionnaire_status,
    qn.question_content,
    qn.question_type,
    ans.answer_type,
    ans.date AS answer_date
FROM
    organization org
LEFT JOIN
    questionnaire qnnr ON org.id = qnnr.organization_id
LEFT JOIN
    question qn ON qnnr.id = qn.questionnaire_id
LEFT JOIN
    answer ans ON qn.id = ans.question_id
GROUP BY
    qn.id,
    qnnr.theme_id,
    qn.creation_date,
    org.id,
    org.name,
    org.country,
    org.datecreated,
    org.datemodified,
    org.status,
    qnnr.name,
    qnnr.datecreated,
    qnnr.datemodified,
    qnnr.type,
    qnnr.status,
    qn.question_content,
    qn.question_type,
    ans.answer_type,
    ans.date
ORDER BY
    org.id, qn.id;

