/* AGGREGATE ARTICLE QUALITY RESULTS FOR EACH QUALITY CRITERION 

We are interested in a breakdown according to article type - paid, not paid, total inventory
*/


--- **** THE FOLLOWING TABLE IS HISTORIZED AND SHOULD NOT BE MODIFIED BEFORE MAKING A SAFETY COPY
CREATE TABLE IF NOT EXISTS ANALYTICS_DB.AC_PERCENTAGES_{week_or_month!i}LY (
  INV_CATEGORY                          VARCHAR(50) UTF8   COMMENT IS 'all inventory, paid or not paid',
  PERIOD_ID                             DECIMAL(6,0),
  PERIOD_START_DAY                      DATE,
  PERIOD_END_DAY                        DATE,                 -- need this for Tableau
  NR_ARTICLES_TOTAL                     DOUBLE PRECISION,
  PERC_IS_REASONABLE_LENGTH             DOUBLE PRECISION   COMMENT IS '% of articles where the article description is min 3,000 characters and max 15,000',
  PERC_IS_DISPLAY_FRIENDLY_TITLE        DOUBLE PRECISION   COMMENT IS '% of articles where article title is max 100 characters',
  PERC_AUTHOR_BIO_SHOWN                 DOUBLE PRECISION   COMMENT IS '% of articles which display a brief author bio',
  PERC_IS_MOBILE_FRIENDLY               DOUBLE PRECISION   COMMENT IS '% of articles which are suited for small screens',
  PERC_IS_FEATURED                      DOUBLE PRECISION   COMMENT IS '% of articles that are featured on the main Camelot page',
  PERC_READER_RATING_SHOWN              DOUBLE PRECISION   COMMENT IS '% of articles which display a reader rating',
  PERC_NO_DUPLICATES                    DOUBLE PRECISION   COMMENT IS '% of articles which have no duplicates (duplicate = articles with identical article title and author name shown at the same time)',
  PERC_ARTICLE_CATEGORY_DETECTED        DOUBLE PRECISION   COMMENT IS '% of articles where an article category has been detected via either the article title or tags',
  ETL_COMPUTED_AT_DATETIME              TIMESTAMP          COMMENT IS 'timestamp when this entry was computed'

) COMMENT IS 'KPIs for measuring article completeness PERCENTAGES';



--INSERT DATA -  PAID
INSERT INTO ANALYTICS_DB.AC_PERCENTAGES_{week_or_month!i}LY
    SELECT
        'PAID'                                              as INV_CATEGORY,
        AC.PERIOD_ID                                        as PERIOD_ID,
        A.{week_or_month!i}_START_DAY                       as PERIOD_START_DAY,
        A.{week_or_month!i}_END_DAY                         as PERIOD_END_DAY,        -- need this for tableau
        COUNT(ARTICLE_ID)                                   as N_ARTICLES_TOTAL,
        sum(IS_REASONABLE_LENGTH)/ COUNT(ARTICLE_ID)        as PERC_IS_REASONABLE_LENGTH,
        sum(IS_DISPLAY_FRIENDLY_TITLE) / COUNT(ARTICLE_ID)  as PERC_IS_DISPLAY_FRIENDLY_TITLE,
        sum(AUTHOR_BIO_SHOWN) / COUNT(ARTICLE_ID)           as PERC_AUTHOR_BIO_SHOWN,
        sum(IS_MOBILE_FRIENDLY) / COUNT(ARTICLE_ID)         as PERC_IS_MOBILE_FRIENDLY,
        sum(IS_FEATURED) / COUNT(ARTICLE_ID)                as PERC_IS_FEATURED,
        sum(READER_RATING_SHOWN) / COUNT(ARTICLE_ID)        as PERC_READER_RATING_SHOWN,
        sum(NO_DUPLICATES) / COUNT(ARTICLE_ID)              as PERC_NO_DUPLICATES,
        sum(ARTICLE_CATEGORY_DETECTED) / COUNT(ARTICLE_ID)  as PERC_ARTICLE_CATEGORY_DETECTED,
        current_timestamp                                   as ETL_COMPUTED_AT_DATETIME


    FROM ANALYTICS_DB.AC_ANALYSIS_{week_or_month!i}LY AC
    INNER JOIN DIMENSIONS.DIM_{week_or_month!i} A
    ON AC.PERIOD_ID = A.{week_or_month!i}_ID
    WHERE IS_PREMIUM = 1
    GROUP BY 1, 2, 3, 4
;


--INSERT DATA -  PAID
INSERT INTO ANALYTICS_DB.AC_PERCENTAGES_{week_or_month!i}LY
    SELECT
        'NOT PAID'                                          as INV_CATEGORY,
        AC.PERIOD_ID                                        as PERIOD_ID,
        A.{week_or_month!i}_START_DAY                       as PERIOD_START_DAY,
        A.{week_or_month!i}_END_DAY                         as PERIOD_END_DAY,        -- need this for tableau
        COUNT(ARTICLE_ID)                                   as N_ARTICLES_TOTAL,
        sum(IS_REASONABLE_LENGTH)/ COUNT(ARTICLE_ID)        as PERC_IS_REASONABLE_LENGTH,
        sum(IS_DISPLAY_FRIENDLY_TITLE) / COUNT(ARTICLE_ID)  as PERC_IS_DISPLAY_FRIENDLY_TITLE,
        sum(AUTHOR_BIO_SHOWN) / COUNT(ARTICLE_ID)           as PERC_AUTHOR_BIO_SHOWN,
        sum(IS_MOBILE_FRIENDLY) / COUNT(ARTICLE_ID)         as PERC_IS_MOBILE_FRIENDLY,
        sum(IS_FEATURED) / COUNT(ARTICLE_ID)                as PERC_IS_FEATURED,
        sum(READER_RATING_SHOWN) / COUNT(ARTICLE_ID)        as PERC_READER_RATING_SHOWN,
        sum(NO_DUPLICATES) / COUNT(ARTICLE_ID)              as PERC_NO_DUPLICATES,
        sum(ARTICLE_CATEGORY_DETECTED) / COUNT(ARTICLE_ID)  as PERC_ARTICLE_CATEGORY_DETECTED,
        current_timestamp                                   as ETL_COMPUTED_AT_DATETIME


    FROM ANALYTICS_DB.AC_ANALYSIS_{week_or_month!i}LY AC
    INNER JOIN DIMENSIONS.DIM_{week_or_month!i} A
    ON AC.PERIOD_ID = A.{week_or_month!i}_ID
    WHERE IS_PREMIUM = 0
    GROUP BY 1, 2, 3, 4
;



--INSERT DATA - ALL INVENTORY
INSERT INTO ANALYTICS_DB.AC_PERCENTAGES_{week_or_month!i}LY
    SELECT
        'ALL INVENTORY'                                     as INV_CATEGORY,
        AC.PERIOD_ID                                        as PERIOD_ID,
        A.{week_or_month!i}_START_DAY                       as PERIOD_START_DAY,
        A.{week_or_month!i}_END_DAY                         as PERIOD_END_DAY,        -- need this for tableau
        COUNT(ARTICLE_ID)                                   as NR_ARTICLES_TOTAL,
        sum(IS_REASONABLE_LENGTH)/ COUNT(ARTICLE_ID)        as PERC_IS_REASONABLE_LENGTH,
        sum(IS_DISPLAY_FRIENDLY_TITLE) / COUNT(ARTICLE_ID)  as PERC_IS_DISPLAY_FRIENDLY_TITLE,
        sum(AUTHOR_BIO_SHOWN) / COUNT(ARTICLE_ID)           as PERC_AUTHOR_BIO_SHOWN,
        sum(IS_MOBILE_FRIENDLY) / COUNT(ARTICLE_ID)         as PERC_IS_MOBILE_FRIENDLY,
        sum(IS_FEATURED) / COUNT(ARTICLE_ID)                as PERC_IS_FEATURED,
        sum(READER_RATING_SHOWN) / COUNT(ARTICLE_ID)        as PERC_READER_RATING_SHOWN,
        sum(NO_DUPLICATES) / COUNT(ARTICLE_ID)              as PERC_NO_DUPLICATES,
        sum(ARTICLE_CATEGORY_DETECTED) / COUNT(ARTICLE_ID)  as PERC_ARTICLE_CATEGORY_DETECTED,
        current_timestamp                                   as ETL_COMPUTED_AT_DATETIME


    FROM ANALYTICS_DB.AC_ANALYSIS_{week_or_month!i}LY AC
    INNER JOIN DIMENSIONS.DIM_{week_or_month!i} A
    ON AC.PERIOD_ID = A.{week_or_month!i}_ID
    GROUP BY 1, 2, 3, 4
;



-- GET LATEST DATA FOR TABLEAU
CREATE OR REPLACE TABLE ANALYTICS_DB.AC_PERCENTAGES_TABLEAU_{week_or_month!i}LY AS

    -- GET ALL INFORMATION EXCEPT ROW_NR
    SELECT
        INV_CATEGORY,
        PERIOD_ID,
        PERIOD_START_DAY,
        PERIOD_END_DAY,
        NR_ARTICLES_TOTAL,
        PERC_IS_REASONABLE_LENGTH,
        PERC_IS_DISPLAY_FRIENDLY_TITLE,
        PERC_AUTHOR_BIO_SHOWN,
        PERC_IS_MOBILE_FRIENDLY,
        PERC_IS_FEATURED,
        PERC_READER_RATING_SHOWN,
        PERC_NO_DUPLICATES,
        PERC_ARTICLE_CATEGORY_DETECTED

    FROM

    (
        -- we order entries by time stamp and then select only the top row to get the latest entry for that inv_category and PERIOD_id
        SELECT
            row_number() over (partition by INV_CATEGORY, PERIOD_ID order by ETL_COMPUTED_AT_DATETIME desc) as ROW_NR,
            AC.*
        FROM ANALYTICS_DB.AC_PERCENTAGES_{week_or_month!i}LY AC
        ORDER BY PERIOD_ID, INV_CATEGORY, LOCAL.ROW_NR
    )

    WHERE ROW_NR =1
    ORDER BY PERIOD_ID DESC, INV_CATEGORY
;
