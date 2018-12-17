# Import required packages
library(caret)
library(factoextra)
library(fpc)
library(coop)
library(tidyverse)
library(cluster)
library(psych)
library(e1071)
library(ggplot2)
library(moments)

# Create the dataframe
rm(list=ls())
target_df <- read.csv("C:/Users/sgadn/Desktop/MS - CS/MS - SEM - 4/CSP - 595 BDT/Final Project/target_table.csv",header = TRUE, sep = ",")

names(target_df)
dim(target_df)

hist(target_df$Release_Year,border="blue",col="green" , main = "Histogram of Release Year vs Frequency counts", xlab = "Movie Release Years")
hist(target_df$Revenue,border="blue",col="yellow" , main = "Histogram of Revenue vs Frequency counts", xlab = "Movie Revenue")
hist(target_df$Runtime,border="blue",col="red" , main = "Histogram of Runtime vs Frequency counts", xlab = "Movie Runtime")
hist(target_df$Ratings,border="blue",col="orange" , main = "Histogram of Ratings vs Frequency counts", xlab = "Movie Ratings")

plot(target_df$Release_Year,target_df$Revenue, main = "Plot of Release Year vs Revenue", xlab = "Movie Release Year" , ylab = "Movie Revenue" , pch = 16, col = target_df$Release_Year)
plot(target_df$Ratings,target_df$Revenue, main = "Plot of Ratings vs Revenue", xlab = "Movie Ratings" , ylab = "Movie Revenue" , pch = 16, col = target_df$Ratings)



fviz_nbclust(scale(target_df[,c(7,8,10)]), kmeans, method="wss")

kmeans_model <- kmeans(scale(target_df[,c(7,8,10)]), centers = 3, nstart = 25)
fviz_cluster(kmeans_model, data=target_df[,c(7,8,10)],main="K Means clustering with k=3 clusters (Optimal Clustering)")