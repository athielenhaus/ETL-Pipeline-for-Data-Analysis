/* ARTICLE COMPLETENESS ANALYSIS
-- THE AC Analysis takes into the consideration the LAST VISIBLE STATUS of each article within the given time period.
*/

/* First we get all the distinct article IDs of visible articles in the given period, as well as the last day in the given
period that the article was visible, from the table VISIBLE_ARTICLES_DAILY. */
CREATE OR REPLACE TABLE ANALYTICS_DB.ARTICLES_LAST_VISIBLE_DAYS_{week_or_month!i}LY AS

    SELECT

        DISTINCT V.ARTICLE_ID,
                 D.{week_or_month!i}_ID,      -- Need to keep in case of multi-period analysis
                 max(V.DAY_DATE) OVER (PARTITION BY V.ARTICLE_ID, D.{week_or_month!i}_ID) AS LAST_VISIBLE_DAY_IN_PERIOD

    FROM ARTICLES_DB.VISIBLE_ARTICLES_DAILY as V
    INNER JOIN DIMENSIONS.DIM_DAY as D
    ON D.DAY_DESC = V.DAY_DATE
    INNER JOIN DIMENSIONS.DIM_{week_or_month!i} as W
    ON D.{week_or_month!i}_ID = W.{week_or_month!i}_ID

    WHERE TRUE

    -- Dates
    AND to_date(V.DAY_DATE) >= {start_date}
    AND to_date(V.DAY_DATE) <= {end_date}

    ORDER BY 1, 2
;


/*We check the ARTICLES_T table to get the timestamp which corresponds to the last visible status, based on the last visible day
identified above */
CREATE OR REPLACE TABLE ANALYTICS_DB.LAST_STATUS_FOR_AC_ANALYSIS_{week_or_month!i}LY AS

    SELECT
        -- GROUP BY...
        V.ARTICLE_ID,
        V.LAST_VISIBLE_DAY_IN_PERIOD,
        --NOTE: last status may not be from same week or month, therefore we do NOT group by week / month

        -- Get the timestamp from the last status
        MAX(A.UPDATED_AT) AS DATE_LAST_ARTICLE_STATUS

    FROM ARTICLES_DB.ARTICLES_T A
    INNER JOIN ANALYTICS_DB.ARTICLES_LAST_VISIBLE_DAYS_{week_or_month!i}LY as V
    ON A.ID = V.ARTICLE_ID
    AND SUBSTR(A.UPDATED_AT, 1, 10) <= V.LAST_VISIBLE_DAY_IN_PERIOD    -- STATUS MUST BE FROM SAME OR EARLIER DATE AS LAST VISIBLE DAY
    AND A.VISIBILITY_ASPECTS = 0      -- 0 means article is visible

    GROUP BY 1, 2
    ORDER BY 1, 2;



-- Collect information for AC Analysis
CREATE OR REPLACE TABLE ANALYTICS_DB.AC_ANALYSIS_PREP_TABLE_{week_or_month!i}LY  AS

    SELECT DISTINCT
        A.ID                                    AS ARTICLE_ID,
        T.IS_PREMIUM,
        LS.DATE_LAST_ARTICLE_STATUS             AS DATE_LAST_STATUS,
        LS.LAST_VISIBLE_DAY_IN_PERIOD,        -- necessary for join with duplicate-check in next script
        D.{week_or_month!i}_ID                  AS PERIOD_ID,
        A.AUTHOR_ID,
        A.AUTHOR_NAME || A.TITLE                AS DUP_CHECK,

        -- Article source is not explicitly required for current analysis but can be useful at some point
        Case
            WHEN A."SOURCE" IS NULL THEN 'unknown'
            WHEN A."SOURCE" = 'PC' THEN 'Premium Customer'
            WHEN A."SOURCE" = 'cr' THEN 'Crawled'
        END  as ARTICLE_SOURCE,

        -- Check length of article (remove html and extra spaces)
        IF A.CONTENT IS NOT NULL AND LENGTH (REGEXP_REPLACE(REGEXP_REPLACE(A.CONTENT, '<[^>]*>', ''), ' +', ' '))
            BETWEEN 3000 AND 15000 THEN 1 ELSE 0 ENDIF AS IS_REASONABLE_LENGTH,

        -- Check that article title does not exceed maximum length for flexible display (100 char)
        IF A.TITLE IS NOT NULL AND LENGTH (A.TITLE) <= 120 THEN 1 ELSE 0 ENDIF AS IS_DISPLAY_FRIENDLY_TITLE,

        -- Check for mobile-friendly format
        IF A.IS_GFRAME = 0 THEN 1 ELSE 0 ENDIF AS IS_MOBILE_FRIENDLY,

        -- Check if reader rating is displayed
        IF A.RATING IS NULL THEN 0 ELSE 1 ENDIF AS READER_RATING_SHOWN

        -- Check whether article was featured on website main page
        IF A.PROMINENCE = 5 THEN 1 ELSE 0 ENDIF AS IS_FEATURED

    FROM ARTICLES_DB.ARTICLES_T A

    -- Join with previous table
    INNER JOIN ANALYTICS_DB.ARTICLES_TYPE T
    ON A.ID = T.ARTICLE_ID

    -- Join to only get those rows containing the last article status for the given period
    INNER JOIN ANALYTICS_DB.LAST_STATUS_FOR_AC_ANALYSIS_{week_or_month!i}LY as LS
    ON A.ID = LS.ARTICLE_ID                                 -- MATCH ARTICLE_ID
    AND A.UPDATED_AT = LS.DATE_LAST_ARTICLE_STATUS          -- MATCH UPDATED_AT DATE

    INNER JOIN DIMENSIONS.DIM_DAY as D
    ON D.DAY_DESC = LS.LAST_VISIBLE_DAY_IN_PERIOD

    ORDER BY 1, 2

;


-- LEFT JOIN TO RUN QUALITY ANALYSIS ON REMAINING PARAMETERS FROM OTHER TABLES
CREATE OR REPLACE TABLE ANALYTICS_DB.AC_ANALYSIS_{week_or_month!i}LY  AS
    SELECT

        A.*,

        -- Check for author bio
        IF DA.AUTHOR_BIO_FLAG IS NOT NULL AND DC.AUTHOR_BIO_FLAG != 0 THEN 1 ELSE 0 ENDIF AS AUTHOR_BIO_SHOWN,

        -- Check if article category detected
        IF CD.ARTICLE_CATEGORY_DETECTED = 0 OR JR.ARTICLE_CATEGORY_DETECTED IS NULL THEN 0 ELSE 1 ENDIF AS ARTICLE_CATEGORY_DETECTED,

        -- Check for duplicate articles
        IF D.ID IS NULL THEN 1 ELSE 0 ENDIF AS NO_DUPLICATES,

    FROM ANALYTICS_DB.AC_ANALYSIS_PREP_TABLE_{week_or_month!i}LY as A

    --NOTE: NOT HISTORIZED
    LEFT JOIN EXTERNAL_PARTNERS_DB.DIM_AUTHORS as DA
    ON A.AUTHOR_ID = DA.AUTHOR_ID

    --NOTE: NOT HISTORIZED
    LEFT JOIN ANALYTICS_DB.ARTICLE_CATEGORY_DETECTED_BOOLEAN_{week_or_month!i}LY as CD
    ON A.ARTICLE_ID = CD.ID

    --NOTE: HISTORIZED
    LEFT JOIN ANALYTICS_DB.AC_DUPLICATE_CHECK_{week_or_month!i}LY as D
    ON A.DUP_CHECK = D.ID
    /* Only check for duplicates on last visible day in period - must also join on date because duplicate check table
       contains values for each day of the given period*/
    AND A.LAST_VISIBLE_DAY_IN_PERIOD = D.DAY_DATE


ORDER BY 1, 3, 4
;



