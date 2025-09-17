# Project that I made for course "Python project for data engineering"

This project implements a simple ETL (Extract – Transform – Load) pipeline that collects data about the world’s largest banks from a Wikipedia archive, processes it, and saves it both as a CSV file and in an SQLite database.

The main goal is to automate the entire data workflow – from extraction, through transformation, to loading and querying.

## Features

### Extract

Scrapes data from the table on Wikipedia – [List of largest banks (archived)](https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks)
<img width="524" height="363" alt="image" src="https://github.com/user-attachments/assets/3647a4e9-8bc0-433d-89d8-cc409c80abe3" />


Extracts each bank’s name and market capitalization (in billions of USD).

### Transform

Reads exchange rates from exchange_rate.csv.

Converts market capitalization from USD to GBP, EUR, and INR.

Adds new columns to the DataFrame.

### Load

Saves the transformed data into Largest_banks_data.csv.

Loads the data into an SQLite database (Banks.db) under the table name Largest_banks.

### Query (SQL)

Displays all rows from the table.

Calculates the average market capitalization in GBP.

Retrieves the first 5 bank names.
<img width="789" height="400" alt="image" src="https://github.com/user-attachments/assets/784c819a-d281-4240-ba17-3210900fc6f1" />


### Logging

Every ETL step is logged in code_log.txt with timestamps.
<img width="717" height="674" alt="image" src="https://github.com/user-attachments/assets/84441378-87bf-4c10-b978-9633b38af4fe" />
