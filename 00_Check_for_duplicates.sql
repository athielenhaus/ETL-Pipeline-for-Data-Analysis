
/* CHECK FOR DUPLICATES AMONG THOSE ARTICLES THAT WERE VISIBLE ON THE SAME DATE

  Join with visibility table and group by date
*/

CREATE OR REPLACE TABLE ANALYTICS_DB.AC_DUPLICATE_CHECK_{week_or_month!i}LY as

    SELECT
        v.DAY_DATE,

        -- if there are multiple articles with the same author name and title, we consider them duplicates
        A.AUTHOR_NAME || A.TITLE                    as ID,
        count(*)                                    as ARTICLE_COUNT

    -- NOTE: we only check for duplicates for the most recent version of the article.

    FROM ARTICLES_DB.ARTICLES as A
    JOIN ARTICLES_DB.VISIBLE_ARTICLES_DAILY as V
        on A.id = V.article_id
    JOIN DIMENSIONS.DIM_DAY as DD
        on V.day_date = DD.day_desc

    WHERE TRUE

    AND to_date(V.DAY_DATE) >= {start_date}
    AND to_date(V.DAY_DATE) <= {end_date}

    GROUP BY 1, 2
    HAVING count(*) > 1
    ORDER BY 2 desc;



