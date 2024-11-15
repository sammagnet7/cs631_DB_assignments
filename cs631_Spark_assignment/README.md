# CS631_Spark_assignment

>Note: Extract the 'newsdata.zip' inside ./given/Resources folder
---

## Assignment Overview

In this assignment, we will write PySpark code to process a set of files (articles) in a dataset in parallel to find co-occurrences of entities in articles. For example, if an article mentions Narendra Modi and Rahul Gandhi, we say that the two entities co-occur. To simplify our task, we assume that entity names are single words (Modi, Gandhi, etc.), and we assume no two entities have the same name. The list of entities to consider is provided at the end of this document; we will use it in a constant array in our program.

### Input

We are provided with a set of news articles as JSON files in a zip file named `newsdata.zip` on Moodle, which we should save and unzip into a directory. Each JSON file contains metadata about the article and the article content (article_body) itself.

### Objective

In every article, some entities may be mentioned. Two different entities mentioned in an article are said to co-occur in that article.

### Tasks

1. Create a dataset (RDD) with (entity, article_id) pairs where the article contains the entity. We must ensure there are no duplicate rows. We will tokenize the input and perform case-insensitive comparison; the word count program example will help us with this. We will print the dataset.

2. Convert the dataset to a Spark DataFrame (with appropriate column names).

3. Use SparkSQL functions on DataFrames (not SQL itself) to create a DataFrame containing (entity, count) pairs indicating how many articles each entity occurs in. Entities that do not occur in any article should not be included in the output. We will print the DataFrame.

4. As in the previous task, we will do this for all pairs of the given entities, instead of single entities.

5. Find the top-10 entity pairs based on their co-occurrence counts.
