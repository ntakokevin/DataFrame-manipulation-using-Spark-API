# DataFrame-manipulation-using-Spark-API

In this project, we will focus on introducing the DataFrames API in Spark. A DataFrame in Spark consists of Row objects and Column objects. Each row represents a record, while each column represents a specific attribute or field of the data. We will explore the functionalities of these classes in detail.

## Creating DataFrames

DataFrames can be created in multiple ways, including:
<ul>
<li>From Python objects</li>
<li>From external data sources</li>
<li>From other Spark objects</li>
</ul>

When creating DataFrames, you have the option to provide a schema or let Spark infer the schema. A schema defines the column names and associated data types for a DataFrame. Using a schema offers several advantages, such as improved speed, avoiding the need for Spark to infer data types, detecting errors early, and facilitating handling large datasets.

## Defining Schemas

Schemas can be defined programmatically using Spark DataTypes or through Data Definition Language (DDLs). Using Spark DataTypes allows you to define the schema directly in your code, while DDLs provide a way to define the schema using SQL-like syntax. In this project, there will be exercises where you will define schemas for specific datasets.

## Reading CSV Files with Schema

One exercise in this project involves reading a CSV file with a defined schema. The provided data file, "blog_simple_dataset.csv," will be read into a Spark DataFrame using the defined schema. This exercise aims to demonstrate the advantages of using a predefined schema for reading structured data from external sources. You will compare the loading times with and without schema inference.

## Working with External Data Sources

Spark supports reading data from various external data sources. The most common approach, which we have already encountered, is using the DataFrameReader object. The Spark DataFrameReader object provides a wide range of options for reading data from different data stores. You can refer to the Spark documentation to explore the supported data sources and their specific configurations.

## Columns and Expressions in DataFrames

Columns in Spark DataFrames behave similarly to pandas DataFrames. You can access columns by their names, perform operations on their values using relational or computational expressions, add or rename columns, subset data based on columns, compute statistics, and sort data based on column values. The col() function and the expr() function from pyspark.sql.functions are commonly used for these operations.

### Rows and Row Objects

A row in Spark is represented by the Row object, which contains one or more columns. Each column can have the same or different data types. Row objects can be instantiated and collected into a list to create a Spark DataFrame.

## How Spark SQL Executes Computations

The project includes an exercise where you will use the explain() function to understand how the Spark SQL engine plans and executes computations. This information is valuable for complex and time-consuming jobs, as understanding the logical plan before running the job can help optimize performance.

## Interacting with DataFrames using SQL

Spark provides the ability to interact with DataFrames using SQL commands. You can create a DataFrame from an external source, create a temporary view of the DataFrame, and then issue SQL queries using Spark SQL. This allows you to use familiar SQL commands to query and manipulate the data in the DataFrame.

Through this project, we aim to familiarize you with the DataFrames API in Spark. By exploring various operations such as creating DataFrames, defining schemas, reading from external data sources, working with columns and expressions, handling rows, understanding computation execution plans, and utilizing SQL commands, you will gain a comprehensive understanding of the Spark DataFrames API. The project will provide clear instructions, code snippets, and exercises to help you practice and apply your knowledge effectively.
