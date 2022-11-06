# Analysis-using-Big-Data-Tools-and-Techniques

This project involves analysis of Clinical trials data using three implementations - Pyspark(RDD and Dataframes), Hive-SQL on Azure data bricks along with AWS services(S3, Glue and Athena) on AWS platform. 

Following are the problem statements that will be implemented with different techniques

1. The number of studies in the dataset. You must ensure that you explicitly check distinct studies.
2. Listing the types (as contained in the Type column) of studies in the dataset along with
the frequencies of each type. These should be ordered from most frequent to least frequent.
3. The top 5 conditions (from Conditions) with their frequencies.
4. Each condition can be mapped to one or more hierarchy codes. The client wishes to know the 5
most frequent roots (i.e. the sequence of letters and numbers before the  rst full stop) after this is
done.
5. Find the 10 most common sponsors that are not pharmaceutical companies, along with the number
of clinical trials they have sponsored. Hint: For a basic implementation, you can assume that the
Parent Company column contains all possible pharmaceutical companies.
6. Plot number of completed studies each month in a given year { for the submission dataset, the year
is 2021. 


Data loading has been done as part of dataloading.ipynb

