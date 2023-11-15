/* FOR EACH ARTICLE VISIBLE IN GIVEN TIME PERIOD, DETERMINE WHETHER A ARTICLE CATEGORY WAS DETECTED
*/


-- Create table to get category detection status for each article in the given time period
CREATE OR REPLACE TABLE ANALYTICS_DB.ARTICLE_CATEGORY_DETECTED_BOOLEAN_{week_or_month!i}LY AS

    -- Get all distinct articles visible in the given time period
    With distinct_articles_tbl AS
            (SELECT DISTINCT A.ID
              FROM ARTICLES_DB.ARTICLES A

            -- Inner join to get only the visible articles
            INNER JOIN ARTICLES_DB.VISIBLE_ARTICLES_DAILY V
            ON A.ID = V.ARTICLE_ID
            WHERE TRUE
                AND to_date(V.DAY_DATE) >= {start_date}
                AND to_date(V.DAY_DATE) <= {end_date}
            )

    -- Need to select DISTINCT because ARTICLE_ID || ENTITY_TYPE can occur multiple times in each table (an article can be in multiple categories)
    SELECT DISTINCT
        A.ID,

        -- If neither of the Hive tables contains an entry for the given article ID, no category detected
        CASE WHEN T.ARTICLE_ID IS NULL AND G.ARTICLE_ID IS NULL THEN 0 ELSE 1 END AS ARTICLE_CATEGORY_DETECTED

    FROM distinct_articles_tbl A

    -- Below we are left-joining because we want to check for all article IDs from distinct_articles_tbl
    -- Check for category detection from article title
    LEFT JOIN HIVE_DB_ENTITIZED.ARTICLES_TITLE T
    ON A.ID = T.ARTICLE_ID
    AND T.ENTITY_TYPE = 'ARTICLE_CATEGORY'

    -- Check for category detection from article tags
    LEFT JOIN HIVE_DB_ENTITIZED.ARTICLES_TAG G
    ON A.ID = G.ARTICLE_ID
    AND G.ENTITY_TYPE = 'ARTICLE_CATEGORY'

;
