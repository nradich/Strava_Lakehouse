#### Build a data  lakehouse using the Strava API and Azure DataBricks

![alt text](CoverPhoto.png)

Necessary for this project:
 - A Strava developers account
 - DataBricks environment

This repo provider users with the ability to query their own recorded activites within the Strava application 
and then store the details within DBFS.

**utils.py** holds the data cleansing and extraction patterns for the API calls.
**query_orchestration.ipynb** organizes the API calls, schedule this file to run.
*config.py* holds authentication and storage path variables.
*Notebooks* is a folder holding scratch work used to formulate the extraction patterns for each API call.







