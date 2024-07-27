# Airflow 2.0

This repository contains an Apache Airflow DAG designed to analyze video game sales data.

## DAG Overview

The DAG `e_krasnobaeva_2` performs the following tasks:
1. **Load Data**: Loads video game sales data from a provided CSV link.
2. **Top Game**: Identifies the best-selling game globally for a specific year.
3. **Top Genre in EU**: Finds the genres with the highest sales in the EU.
4. **Top Platform in NA**: Determines the platforms with the highest sales in North America.
5. **Top Publisher in JP**: Identifies the publishers with the highest average sales in Japan.
6. **Games Sold Better in EU than JP**: Counts the number of games that sold better in the EU than in Japan.
7. **Print Results**: Outputs the results of the analysis.

## Usage

1. **Install Dependencies**: Ensure you have Apache Airflow and Pandas installed.
2. **Add DAG to Airflow**: Place the `e_krasnobaeva_2.py` file in your Airflow DAGs directory.
3. **Run the DAG**: Trigger the DAG from the Airflow UI.

## Code Explanation

The script uses the `@dag` and `@task` decorators from the `airflow.decorators` module to define the DAG and its tasks. The `default_args` dictionary sets the default parameters for the DAG, such as the owner, retry settings, and schedule interval.

The data is loaded from a CSV link and filtered for a specific year based on a hash of the username. Various tasks process the data to extract insights, which are then printed in the final task.

## Link to Data

The data used in this analysis can be found [here](https://kc-course-static.hb.ru-msk.vkcs.cloud/startda/Video%20Game%20Sales.csv).

## Contact

For any questions or additional information, please contact me at elina8kr@gmail.com.
My LinkedIn: [https://www.linkedin.com/in/elina-krs/](https://www.linkedin.com/in/elina-krs/)

## Key Skills

- Python
- Pandas
- Airflow 
- Data Analysis
