# DataMiningStats

This project is divided into 3 parts with each part explained below. We use Yelp dataset for each of the parts.

Part1:
Exploring the dataset, review.json, containing review information.
Program to automatically answer the following questions:
A. The total number of reviews
B. The number of reviews in 2018
C. The number of distinct users who wrote reviews
D. The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote
E. The number of distinct businesses that have been reviewed
F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had

Part2:
Since processing large volumes of data requires performance decisions, properly partitioning the data for
processing is imperative.
In this task, we show the number of partitions for the RDD used for Part 1 (F) and the number
of items per partition.
Then we use a customized partition function to improve the performance of map and reduce tasks. 
A time duration comparison between the default partition and the customized partition 
(RDD built using the partition function) is shown in results. 

Part3:

Exploring two datasets together containing review information (review.json) and
business information (business.json) and write a program to answer the following questions:
A. What is the average stars for each city?
B. Using two ways to print top 10 cities with highest stars.
Method1: Collect all the data, sort in python, and then print the first 10 cities
Method2: Sort in Spark, take the first 10 cities, and then print these 10 cities
