# Data-Engineering
This repo shall host all tasks and projects with regards to Data Engineering. 

# Project 1

## Data Warehousing Pipeline-Cloud(AWS RedShift)

### Project Description

The main objective of this project was to create a pipeline of retrieving data from a Data Lake and creating tables in order for it to be loaded into a Data Warehouse. Amazon Web Services was the cloud service provider used for this project. 

### Datasets

The dataset used in this project consist of six(6) text files as pipe delimiterd values. The dataset in general consists of data of ticket sales and it's associated attribute information. The 6 datasets are:

1. allevents_pipe.txt
2. allusers_pipe.txt
3. category_pipe.txt
4. date2008_pipe.txt
5. listings_pipe.txt
6. venue_pipe.txt

#### A sample of the dataset can be seen below:

![Sample Dataset](https://github.com/Gregory-Essuman/Data-Engineering/blob/main/assets/venue_pipe.JPG)

### Files

1. The notebook file has the code for an end to end data pipeline on the cloud.

2. The cluster.config file is also used by the notebook to parse environment variables for the pipeline. Configparser module in python helps with that.

Architecture used includes:

1. AWS S3 For Storage
2. AWS Redshift For Data Warehousing

### Database Schema

The dataset came in a partially relational schema and was loaded to the data warehouse in that structure since the main objective of this project was not on star schemas. An ideal star schema for the dataset was however designed and broken down into fact and dimension tables. Below are visual guides of the dataset's schema in its current state and the ideal star schema for it.
-------------------------------
#### Current Schema of Dataset:
-------------------------------


![Cuurent Schema of Dataset](https://github.com/Gregory-Essuman/Data-Engineering/blob/main/assets/Ticketsdb%20(2).jpg)

Ideal Star Schema Should Consist of:

- 5 Dimension Tables
- 1 Fact Table
---------------------------------
#### Ideal Star Schema of Dataset
---------------------------------


![Ideal Star Schema of Dataset](https://github.com/Gregory-Essuman/Data-Engineering/blob/main/assets/Ticketsdb%20(1).jpg)



# Project 2 

## Data Modelling Pipeline (PostgreSQL)
