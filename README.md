# Introduction

This repo is the home for all ELT-related assets (inclusive but not limited to LEI, ESEF, ISIN etc.)

# Files and Folders

The root folder of this repo contains executable Airflow DAG scripts. As of this moment, there are DAG scripts as follows

- *gleif_full_reload_dag.py* - the DAG script to facilitate full reload of the latest GLEIF (LEI) data on the daily basis
- *isin_full_reload_dag.py* - the DAG script to facilitate the full reload of the latest ISIN-to-LEI relationship data on the daily basis
- *esef_downloader.py* - the ESEF Reports downloader DAG
- *esef_parser.py* - the ESEF Reports parser DAG
- *isin_export_dag.py* - export full ISIN, LEI, LegalName database and upload to Azure storage
- *isin_export_by_country_dag.py* - export ISIN, LEI, LegalName group by country and upload to Azure storage
- *isin_export_by_date_dag.py* - export ISIN, LEI, LegalName database from last day and upload to Azure storage

There are also subfolders as follows

- *dependencies* contains the auxiliary Python lib files reused by DAG scripts as well as SQL queries used in ETL pipelines
- *dataproc* contains the Dataproc workflow templates with PySpark jobs used by certain operators in DAG scripts to quickly process BigData-scale datasets
- *docs* subfolder contains UML diagrams along with other essential technical design and user guide documents relevant for  xyz Digital's ETL flow
- *notebooks* contains the early-stage experimental notebooks where the certain elements of the Dataproc workflow jobs were prototyped/tested (the assets in this folder can be reviewed for the historical reasons mainly)

# Feature-Specific Notes

**Note:** The features described in the subsections below are in POC (prototype) phase as of Jun 24, 2021

## LEI (GLEIF) Data Flow

More details about LEI workflow, along with the implementation notes, are provided by the Confluence documents below

- _LEI Data Flow_(https://confluence.xyz.com/display/SE/LEI+Data+Flow)
- [PoC LEI Data Product Prototype: Implementation Notes](https://confluence.xyz.com/display/SE/PoC+LEI+Data+Product+Prototype%3A+Implementation+Notes)

## ISIN Data Flow

More details about ISIN Data flow provided in the comments to https://jira.xyz.com/browse/SE-50

## ESEF Reports Data Flow

You can find more details about ESEF Reports ETL/Data flow in the Confluence page below

- [PoC | Data Collector - Automatically collect & activate data for ESEF reports](https://confluence.xyz.com/pages/viewpage.action?pageId=12264137)
