# Data-Engineering
This repo shall host all tasks and projects with regards to Data Engineering. 

# Project 1

## Data Warehousing Pipeline-Cloud(AWS RedShift)

### Project Description

The main objective of this project was to create a pipeline of retrieving data from a Data Lake and creating fact and dimension tables in order for it to be loaded into a Data Warehouse. Amazon Web Services was the cloud service provider used for this project. 

### Datasets

The dataset used in this project consist of six(6) text files as pipe delimiterd values. The dataset in general consists of data of ticket sales and it's associated attribute information. The 6 datasets are:

1. allevents_pipe.txt
2. allusers_pipe.txt
3. category_pipe.txt
4. date2008_pipe.txt
5. listings_pipe.txt
6. venue_pipe.txt

A sample of the dataset can be seen below

![Sample Dataset](https://github.com/Gregory-Essuman/Data-Engineering/blob/main/assets/venue_pipe.JPG)

### Files

The notebook file has the code for an end to end data pipeline on the cloud. 

Architecture used includes:

1. AWS S3 For Storage
2. AWS Redshift For Data Warehousing

### Star Schema

The dataset was broken down into fact and dimension tables upon loading to the data warehouse. Below is a visual guide to how the dataset was put to a star schema optimized for data warehouse purposes. The star schema includes:

- 5 Dimension Tables
- 1 Fact Table
