# ETL Pipeline for News Aggregator with SQL + Python Airflow

### Scenario
Our objective is to create a series of scripts which will allow us to conduct a weekly and monthly analysis of online articles for a (fictional) online news aggregator and entertainment website called Camelot. 

Camelot collects articles from paying customers in return for "premium" promotion on the Camelot news portal, as well as from other sources.

### Article Completeness Criteria:

The objective is to collect and track data on the extent to which articles meet certain criteria, which can impact their display and user engagement. These criteria include:

- length of article title is less than 120 characters (important because longer titles may get cut off in certain display formats)
- length of article content is greater than 3,000 and less than 15,000 characters
- article includes brief author bio
- article category was detected by in-house algorithms (important for searches within the Camelot portal)
- a reader rating is displayed (depends on the display format)
- article is prominently featured
- technical format is mobile friendly (important because some article formats are less suited for small screens)
- article has duplicates published at the same time (can happen because multiple methods are used for collecting articles)

The objective is to determine the number of postings which meet the above criteria on a weekly / monthly basis, and to load the corresponding data into tables which can be used for a Tableau Dashboard. Furthermore, a "Article Completeness Score" is to be created which indicates the extent to which an article fulfills the above criteria. 

### Overview of solution: 

![DAG Chart](./DAG.png)

To collect and aggregate the data, several SQL scripts were created, which can be run sequentially using the DAG.py script which contains an Airflow DAG. The first two SQL scripts (beginning with 00) are run in parallel, the subsequent scripts must be run in order. The same SQL scripts are used for both the weekly and monthly analysis. The related variables related to "weekly" and "monthly" analysis are passed into the scripts by the DAG.py script. The weekly script runs every Monday and the monthly script every first day of the month, at noon, after other upstream scripts have been run.

Scripts overview:
- 00 Check for detected article category: check tables to determine, for each article, whether article category was detected  
- 00 Check for duplicates: see which articles have an identical author and article title
- 01 For each article visible during the given time period, check whether it meets each of the article completeness criteria
- 02 Aggregate results according to categories (paid, not paid, all inventory) for each of the criteria. Our result is a table which indicates the percentage of articles in the given period which meeting each AC criterion.
- 03 Calculate Article Completeness score (AC score) for each posting and aggregate results according to categories (paid, not paid, all inventory). Our result is a table which indicates the mean and median AC score for the given period. 

Since all criteria are booleans, i.e. they are either met or not met, a simple score is created where each criterion met accounts for 1 point. An article that meets 5 out of 8 criteria thus receives 5/8 points.

### Conclusion and Next Steps
While this is a fictional scenario, it is based on my previous professional experiences. 
Here are some possible next steps:

__ML Model creation:__ an ML model could be created to predict article engagement (views, clicks, shares, etc.) based on the indicated criteria. This could involve:
  - creating a regression model and calculating adjusted r2, to determine the amount of variance which can be explained by the features
  - conducting a feature importance analysis, utilizing for example permutation feature importance and partial dependence plots, to determine the features that have the most significant impact

Creating a model can help us determine those features which likely create the most value in terms of engagement. We can then adjust our optimization efforts accordingly and avoid spending time on features that provide little to no value to our users.

__DAG Optimization:__  
it is always irritating when you look at your Tableau Dashboard, see some strange data behavior, and then go on a time-consuming hunt for the cause, only to find that something went wrong in your data pipeline or upstream. Whenever a DAG is run frequently, it can be worthwhile to:
  - integrate some basic QA checks  which help indicate to what extent the processes have run successfully (for example: if the length of a table we create as part of the DAG does not exceed a predetermined minimum, raise an error)
  - integrate external event sensors to detect successful completion of upstream DAGs
  - discuss with Data Engineering Dept. or owners of upstream DAGs possible / emerging data quality issues



