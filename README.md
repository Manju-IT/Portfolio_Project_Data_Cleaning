** SQL Project - Data Cleaning **

This project involves cleaning and standardizing a dataset of layoffs from 2022. The goal is to ensure the data is ready for analysis by removing duplicates, handling missing values, standardizing data formats, and removing unnecessary rows and columns.

Dataset:
Source: Kaggle Layoffs 2022 Dataset

Table: world_layoffs.layoffs

Steps Involved
1. Create Staging Table:
Preserve the raw data and work on a copy for cleaning.

2. Remove Duplicates:
Identify and remove duplicate records by examining key columns.

3. Standardize Data:
Convert empty strings to NULL.

Update NULL values in the industry column based on other rows with the same company name.

Standardize variations in categorical data, such as industry names.

Fix country names and date formats.

4. Handle Null Values:
Review and decide how to handle null values in key columns.

5. Remove Unnecessary Rows and Columns:
Remove rows with insufficient data and drop any helper columns used during cleaning.

Final Steps:
Validate the cleaned data to ensure it is consistent and ready for analysis.

Proceed with further analysis and visualization using the cleaned dataset.

Conclusion:
This project demonstrates the process of data cleaning in SQL, ensuring the dataset is prepared for effective analysis. The steps involved highlight common data cleaning tasks such as removing duplicates, handling null values, and standardizing data formats.

