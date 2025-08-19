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

These values can be centralized in a config.py file for easier management.

# Streamlit Dashboard

* The main entry point is [app.py](app.py), which provides:

* File browser for available scripts in HDFS

* Script preview before execution

* Multi-select to run multiple scripts

* Retry configuration for failed jobs

* Execution summary with time taken & results

# Logs

Logs are handled with Python’s logging system:

Configurable log level (INFO, DEBUG, etc.)

File output: [loggsetup.py](loggsetup.py)

Format: timestamp - log level - message

# Execution Manager

Main orchestration logic resides in: [execution-manager.py](execution-manager.py)

This module manages the execution flow of Python scripts stored in HDFS and records run information in MongoDB.

🔹 Features:

HDFS Access – Fetches and lists available .py scripts from HDFS.

MongoDB Logging – Stores job metadata and updates run states (queued, running, completed, failed).

Streamlit Dashboard –

Preview script contents

Choose scripts to run

Configure number of retries

Spark Integration – Submits selected jobs to Spark using spark-submit for distributed execution.

Failure & Retry Logic – Failed jobs are retried automatically based on user-defined limits.

Provides a Streamlit-powered interface to monitor, preview, and re-run jobs with ease.

# Installation

Set up the environment by installing the dependencies from requirements.txt
