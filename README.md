# Project: Trade Data ETL
## Introduction
> Maven Project that builds ETL pipeline with Java and Spring Batch.

The objective of the this project is to read the trade data from a single CSV file, process in certain steps and load the data into multiple CSV files based on data attributes.

## Input
##### input.csv in resources directory

## Data Model
1. Trade ID: The unique identifier to recognize the trade
2. Term: Number of days before bond expires
3. Trade Value: Total value of the trade in given currency
4. Currency: Currency of the trade

## Data Processing
1. If the trade term is below 10 years, output a single row of record and convert the term days with the term bucket.
2. If the trade term is beyond 10 years, split the trade into multiple records following the descending order of term buckets
    | Bucket | Starting Date | End Date |
    |--------|---------------|----------|
    | 3M     | 0             | 90       |
    | 6M     | 91            | 180      |
    | 1Y     | 181           | 365      |
    | 2Y     | 366           | 730      |
    | 5Y     | 731           | 1,825    |
    | 10Y    | 1,826         | 3,650    |

For example, a single row of trade data is given
| Trade ID          | Term | Trade Value | Currency |
|-------------------|------|-------------|----------|
| 123456            | 5402 | 107833      | CAD      |
| After Processing: |      |             |          |
| 123456            | 10Y  | 72860       | CAD      |
| 123456            | 5Y   | 34973       | CAD      |

## Data Output:
##### Output_Currency.csv in the root directory