# IMDB Top 10 Movies

This application is a streaming solution that analyzes the IMDB dataset to answer the following questions:

1. Retrieve the top 10 movies with a minimum of 500 votes with the ranking determined by:
   (numVotes/averageNumberOfVotes) * averageRating

2. For these 10 movies, list the persons who are most often credited and list the
   different titles of the 10 movies.

## Requirements

- Python
- Apache Spark (PySpark)

## Dataset

The dataset can be downloaded from IMDB: https://datasets.imdbws.com/

## Setup

1. Clone the repository: git clone https://github.com/

2. Download the IMDB dataset and place it in the `imdb-datasets/large` directory.

3. Ensure you have Python installed on your system.

4. Install PySpark using `pip`: pip install pyspark

Note: If you encounter any issues with the installation, refer to the official PySpark documentation for detailed installation instructions specific to your operating system.

## Execution

To run the application: python imdb-toyota.py

The application will output the top 10 movies with the required information.
