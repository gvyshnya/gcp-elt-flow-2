SELECT
    e.LEI,
    e.Exception_Category,
    e.Exception_Reason_1,
    e.Exception_Reason_2,
    e.Exception_Reason_3,
    e.Exception_Reason_4,
    e.Exception_Reason_5,
    e.Exception_Reference_1,
    e.Exception_Reference_2,
    e.Exception_Reference_3,
    e.Exception_Reference_4,
    e.Exception_Reference_5
FROM `{{params.project_name}}.lei_staging.Exceptions` e