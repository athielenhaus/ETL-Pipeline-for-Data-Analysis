
# IMPORT LIBRARIES
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, coalesce

def run_cat_check(start_date, end_date):
    # Initialize a Spark session
    spark = SparkSession.builder.appName("ArticleCategoryDetection").getOrCreate()

    # Reading data from ARTICLES and VISIBLE_ARTICLES_DAILY tables
    articles_df = spark.table("ARTICLES_DB.ARTICLES")
    visible_articles_daily_df = spark.table("ARTICLES_DB.VISIBLE_ARTICLES_DAILY")

    # Perform inner join to get only the visible articles within the given time period
    visible_articles = articles_df.join(
        visible_articles_daily_df,
        articles_df.ID == visible_articles_daily_df.ARTICLE_ID
    ).where(
        (to_date(visible_articles_daily_df.DAY_DATE) >= start_date) &
        (to_date(visible_articles_daily_df.DAY_DATE) <= end_date)
    ).select("ID").distinct()

    # Read data from Hive tables
    articles_title_df = spark.table("HIVE_DB_ENTITIZED.ARTICLES_TITLE")
    articles_tag_df = spark.table("HIVE_DB_ENTITIZED.ARTICLES_TAG")

    # Perform left joins to check for category detection
    result_df = visible_articles.alias('A').join(
        articles_title_df.alias('T'),
        (col('A.ID') == col('T.ARTICLE_ID')) & (col('T.ENTITY_TYPE') == 'ARTICLE_CATEGORY'),
        'left'
    ).join(
        articles_tag_df.alias('G'),
        (col('A.ID') == col('G.ARTICLE_ID')) & (col('G.ENTITY_TYPE') == 'ARTICLE_CATEGORY'),
        'left'
    ).select(
        col('A.ID'),
        when(coalesce(col('T.ARTICLE_ID'), col('G.ARTICLE_ID')).isNull(), 0).otherwise(1).alias('ARTICLE_CATEGORY_DETECTED')
    ).distinct()

    # Show results (for testing )
    result_df.show(20)

    result_df.write.saveAsTable("ANALYTICS_DB.ARTICLE_CATEGORY_DETECTED_BOOLEAN_WEEKLY")