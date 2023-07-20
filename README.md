# Data Eng project

A project that shows how one could extract data from a data source and distribute this data to a stakeholder by developing an analytical dashboard, We will focus only on getting data beyond the external data source, so we are assuming that you already have a data process that dumps data into AWS S3 buckets. You can use fake data to practice the process.

## Overview
Steps that we will see:

# - How to dump and Extract data from some source 
           In this project we will cover how to get dump and get data from aws S3 and another approach using pyspark

# - Dump the data into a SQL managment tool where the B.I analysts can use to develop data analysis 
           we can use redshift managment tool and a python approach to dump data into postgresql
  
## - Connect the databases we Develop and create a dashboard with the data to understand data patterns.
  

The main objective is to show how to create a simple data pipeline and how a data analyst can use some scalabe tools that will work based on the project requirements, the S3 bucket is a widely used tool to storage and distribute data across the organization data systems and redshift is a friendly aws service that is also powerfull to provide data and data accessibility due to aws governance policies, and redshift also allows us to connect and create a Power bi dashboard with our data to answer mapped questions.

![S3/ Redshift Project architecture](rds-spark.png)

  
We will also try to do the same process but using a jupyter notebook using spark and pyspark to get data from s3/test_folder and create a database on postgresql which allows us to connect and create a Power bi dashboard with our data to answer mapped questions. We will use postgresql as a second option to show that we can use different approachs for the same problem and offer different solutions to the stakeholders necessities.

![CSV/Postgresql using spark Project architecture](pyspark.png)
 

## Prerequisites

Directions or anything needed before running the project.

- Aws account 
- Spark installed , to see more information check the apache documentation https://spark.apache.org/docs/latest/
- Postgresql installed , download here https://www.postgresql.org/download/
- Any IDE
- 

## How to Run The s3 redshift data pipeline Project

Replace the example step-by-step instructions with your own.
* You might have several s3 buckets on your organization and you can use those files in this process to create a whole set of databases on redshift
* We are assuming that you do not have any s3 bucket and it starting to know the tools i've mentioned before.
## Create a s3 bucket
1. Open your AWS console and open the s3 service*
2. Click on 'creat bucket'
3. Choose your bucket name 
4. you get get any type of data you want to practice at https://www.kaggle.com/datasets
5. Dump the csv files you have downloaded into the s3 bucket you have created
## Create a redshift cluster  
1. On your AWS console open the redshift service
2. Create a cluster, make sure to use the proper configurations to not get overcharged (check the doc to more details https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-launch-sample-cluster.html)
3. Get your cluster running and open the redshift workbench

## Lessons Learned

It's good to reflect on what you learned throughout the process of building this project. Here you might discuss what you would have done differently if you had more time/money/data. Did you end up choosing the right tools or would you try something else next time?

## Contact

Please feel free to contact me if you have any questions at: LinkedIn, Twitter
