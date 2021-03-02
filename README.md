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

The Content Based 


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




## Running the tests



### And coding style tests

```


```

## Authors

* **Ulderico Vagnoni** - *Collaborative Filtering* - [Uderr](https://github.com/Uderr)

* **Giseppe Di Maria** - *Content Based* - [giuseppdimaria](https://github.com/giuseppdimaria)


