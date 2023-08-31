/* CALCULATE ARTICLE SCORES FOR PERIOD

   PURPOSE:
   -- Calculate the article completeness score for each article
   -- Calculate average (mean) and median AC scores for the given period
   -- Insert these values into a table which serves as source for Tableau Dashboard
 */

-- CREATE AC SCORE - ADD UP THE POINTS FOR EACH ARTICLE AND WEEK
CREATE OR REPLACE TABLE ANALYTICS_DB.ARTICLES_AC_SCORES_{week_or_month!i}LY AS

    (SELECT
        DISTINCT PERIOD_ID, ARTICLE_ID, PAID_TYPE,

    (
        IS_REASONABLE_LENGTH+
        IS_DISPLAY_FRIENDLY_TITLE+
        AUTHOR_BIO_SHOWN+
        IS_MOBILE_FRIENDLY+
        IS_FEATURED+
        NO_DUPLICATES+
        ARTICLE_CATEGORY_DETECTED+
        READER_RATING_SHOWN
        ) as ARTICLE_COMPLETENESS_SCORE

FROM ANALYTICS_DB.AC_ANALYSIS_{week_or_month!i}LY
order by PERIOD_ID, ARTICLE_ID
);



--- **** THIS TABLE IS HISTORIZED -- DO NOT MODIFY BEFORE MAKING A SAFETY COPY
CREATE TABLE IF NOT EXISTS ANALYTICS_DB.AC_SCORE_AVERAGES_{week_or_month!i}LY (
  INV_CATEGORY                   VARCHAR(50) UTF8      COMMENT IS 'all inventory, paid or not paid',
  PERIOD_ID                      DECIMAL(6,0),
  PERIOD_START_DAY               DATE,                 -- need this for tableau
  PERIOD_END_DAY                 DATE,                 -- need this for tableau
  MEAN_SCORE                     DOUBLE PRECISION,
  MEDIAN_SCORE                   DOUBLE PRECISION,
  ETL_COMPUTED_AT_DATETIME       TIMESTAMP             COMMENT IS 'timestamp when this entry was computed'
);


-- ADD VALUES FOR PAID
INSERT INTO ANALYTICS_DB.AC_SCORE_AVERAGES_{week_or_month!i}LY
    SELECT
        'PAID'                                                  as INV_CATEGORY,
        AC.PERIOD_ID                                            as PERIOD_ID,
        T.{week_or_month!i}_START_DAY                           as PERIOD_START_DAY,
        T.{week_or_month!i}_END_DAY                             as PERIOD_END_DAY,
        ROUND(AVG(ARTICLE_COMPLETENESS_SCORE/8), 3)             as MEAN_SCORE,
        ROUND(MEDIAN(ARTICLE_COMPLETENESS_SCORE/8), 3)          as MEDIAN_SCORE,
        current_timestamp                                       as ETL_COMPUTED_AT_DATETIME

    FROM ANALYTICS_DB.ARTICLES_AC_SCORES_{week_or_month!i}LY as AC
    INNER JOIN DIMENSIONS.DIM_{week_or_month!i} as T
    ON AC.PERIOD_ID = T.{week_or_month!i}_ID
    WHERE IS_PREMIUM = 1
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2, 3
;


-- ADD VALUES FOR TOP PAID WEEKLY
INSERT INTO ANALYTICS_DB.AC_SCORE_AVERAGES_{week_or_month!i}LY
    SELECT
        'NOT PAID'                                              as INV_CATEGORY,
        AC.PERIOD_ID                                            as PERIOD_ID,
        T.{week_or_month!i}_START_DAY                           as PERIOD_START_DAY,
        T.{week_or_month!i}_END_DAY                             as PERIOD_END_DAY,
        ROUND(AVG(ARTICLE_COMPLETENESS_SCORE/8), 3)             as MEAN_SCORE,
        ROUND(MEDIAN(ARTICLE_COMPLETENESS_SCORE/8), 3)          as MEDIAN_SCORE,
        current_timestamp                                       as ETL_COMPUTED_AT_DATETIME

    FROM ANALYTICS_DB.ARTICLES_AC_SCORES_{week_or_month!i}LY as AC
    INNER JOIN DIMENSIONS.DIM_{week_or_month!i} as T
    ON AC.PERIOD_ID = A.{week_or_month!i}_ID
    WHERE IS_PREMIUM = 0
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2, 3
;


-- ADD VALUES FOR ALL INVENTORY
INSERT INTO ANALYTICS_DB.AC_SCORE_AVERAGES_{week_or_month!i}LY
    SELECT
        'ALL INVENTORY'                                     as INV_CATEGORY,
        AC.PERIOD_ID                                        as PERIOD_ID,
        A.{week_or_month!i}_START_DAY                       as PERIOD_START_DAY,
        A.{week_or_month!i}_END_DAY                         as PERIOD_END_DAY,
        ROUND(AVG(ARTICLE_COMPLETENESS_SCORE/8), 3)         as MEAN_SCORE,
        ROUND(MEDIAN(ARTICLE_COMPLETENESS_SCORE/8), 3)      as MEDIAN_SCORE,
        current_timestamp                                   as ETL_COMPUTED_AT_DATETIME

    FROM ANALYTICS_DB.ARTICLES_AC_SCORES_{week_or_month!i}LY as AC
    INNER JOIN DIMENSIONS.DIM_{week_or_month!i} as P
    ON AC.PERIOD_ID = A.{week_or_month!i}_ID
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2, 3
;



-- GET LATEST DATA FOR TABLEAU
CREATE OR REPLACE TABLE ANALYTICS_DB.AC_SCORES_TABLEAU_{week_or_month!i}LY AS

    -- GET ALL INFORMATION EXCEPT ROW_NR
    SELECT
        INV_CATEGORY,
        PERIOD_ID,
        PERIOD_START_DAY,
        PERIOD_END_DAY,
        MEAN_SCORE,
        MEDIAN_SCORE

    FROM

    (
        -- we order entries by time stamp and then select only the top row to get the latest entry for that inv_category and week_id
        SELECT
            row_number() over (partition by INV_CATEGORY, PERIOD_ID order by ETL_COMPUTED_AT_DATETIME desc) as ROW_NR,
            AC.*
        FROM ANALYTICS_DB.AC_SCORE_AVERAGES_{week_or_month!i}LY AS AC
        ORDER BY PERIOD_ID, INV_CATEGORY, LOCAL.ROW_NR
    )

    WHERE ROW_NR =1
    ORDER BY PERIOD_ID DESC, INV_CATEGORY
;

