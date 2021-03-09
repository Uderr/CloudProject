# Movies recommenders - Scalable and Cloud Programming 2020/21

A data analysis project for the 2020/21 Scalable and Cloud Programming course of University of Bologna. 
The goal is to recommend films for a new user and for known users according to two recommendation algorithms, the Collaborative Filtering User Based and the Content Based.

### Introduction

Recommendation systems became, in the last years, a common key of various applications, from the e-commerce systems to the streaming on demand platforms.
There are three approaches of recommendation systems: collaborative filtering, content based filtering and hybrid recommender systems.

## Description

In order to work, the algorithms need two dataset, the dataset of movie, which contain informations about movies, and the dataset of ratings of users for a certain movie. 
The datasets used in the project come from MovieLens and they can be downloaded freely from [here](https://grouplens.org/datasets/movielens/).

The Collaborative Filtering is implemented in two ways: in the first way is used the ALS algorithm to factorizate the matrix of the ratings in order to obtain a predictions for a certain user, in the second instead is used the Cosine Similarity, this is used to calculate the similarity of a new user with all the other users in the dataset, after it select 15 movies rated with highest rate by users most similar to the new user.
In order to face the Cold Start problem are implemented in this project three function (CollaborativeFilteringUserBased.scala), that are used to gain informations about a new user asking him to rate some movies (20 top rated movies and if the user want 20 random movies), these rates are saved in a new dataset that can be used during the session.

The Content Based is implemented in an unique way, and it search similarity on attributes of items foreach user. At the end of the execution it returns 20 movies recommendations for 5 different user.

## Environment

The project use Scala 2.12.13 and Spark version 3.0.1, the build.sbt file is the following:

```
name := "ClourProject"

version := "0.1"

scalaVersion := "2.12.13"


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"

```




## Execution

JAR files are required to run the application, once generated these can be run both locally and on the remote cluster.
Services provided by AWS were used in order to run the application remotely.
The services of AWS used to run this project on a remote cluster are:

### S3

Used to create a bucket into which the files used in the execution of the JAR file have been loaded (the databases and the JARs themselves)

### EMR

Used for processing large amounts of data on a remote cluster. By accessing this service you can generate your own cluster by selecting its name, version (in this case emr-6.2.0 as it is compatible with Spark 3.0.1), instance and pair of encryption keys generated with EC2 in order to communicate with the cluster remotely. 
The access to the cluster takes place via an SSH connection which in a Windows environment requires the _Putty_ application.
After the SSH connection, in order to execute the JAR files is used the command:

```
spark-submit --class "classname" "file.jar

```

### EC2

Used for generating the key pair used for communication with the cluster.
In a Windows environment, the service requires the use of _PuttyGen_.



## Authors

* **Ulderico Vagnoni** - *Collaborative Filtering* - [Uderr](https://github.com/Uderr)

* **Giseppe Di Maria** - *Content Based* - [giuseppdimaria](https://github.com/giuseppdimaria)


