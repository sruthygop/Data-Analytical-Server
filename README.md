# DAS (Data Analytics System)

The Data Analytics System (DAS) provides a web-based interface built with Streamlit to manage and run distributed jobs stored in HDFS.
It integrates Apache Spark for execution, MongoDB for status tracking, and a flexible logging system for monitoring job progress and failures.

This project aims to simplify the workflow of executing Python scripts in a distributed environment while giving users a transparent and interactive dashboard.

# Key Highlights

* Interactive UI with Streamlit for job management

* Fetches Python scripts directly from HDFS

* Executes jobs via Spark with retry mechanisms

* Tracks execution status in MongoDB (pending, success, failed)

* Maintains detailed logs in both console & logfile.log

* Allows preview of available .py scripts before running

# Tech Stack

* Language: Python

* Frontend/UI: Streamlit

* Distributed Engine: Apache Spark

* Storage: HDFS

* Database: MongoDB

* Logging & Monitoring: Python logging module

* Job Execution: Subprocess (Spark-submit)

* Data Handling: Pandas

# Workflow

* Connect to HDFS (via InsecureClient) and MongoDB (via MongoClient)

* Scan HDFS directory for available .py scripts

* Insert untracked files into MongoDB with status = pending

* Display list of scripts in the Streamlit UI

* Preview & Select scripts to execute, set retry attempts if needed

* Execute using spark-submit through subprocess

* Update MongoDB status dynamically (success/failed)

* Log all events into logfile.log for traceability

# Configuration

* HDFS URL → http://localhost:50070

* HDFS Directory → /my_projects/scripts

* MongoDB URI → connection string to MongoDB Atlas/local

* Database → project

* Collection → sample

These values can be centralized in a [config.py](config.py) file for easier management.
