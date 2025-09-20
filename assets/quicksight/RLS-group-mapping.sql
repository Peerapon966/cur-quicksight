SELECT DISTINCT CAST("Linked account id" AS VARCHAR) AS "Account ID", GroupName
FROM "cur-quicksight".rls

UNION

SELECT '' AS "Account ID", 'Admin' AS GroupName