# Data pipeline [construction]

Application used to data transform a ANAC databases called 'Tarifas Transporte Aéreo Passageiros Domésticos' by year 2002 to 2005. Starting with a manual extract data on DataSAS (ANAC's site) to saving a datalake. Then process all data in a datalake to a datawarehouse.

<br>
Refinements: Include data 2006 to 2020. 
<br><br>

# Initial Data (Without processing)

It's getting on website (https://sas.anac.gov.br/sas/downloads/view/frmDownload.aspx?tema=14), that uset to csv format.

![ana_portal](./images/anac_portal.png)

# Infrastructure

<br>

1° Active Cloud Storage service.<br>
2° Active Cloud Bigquery.<br>
3° Creating Service Account with ADMIN bigquery ang ADMIN storage permissions.<br>
4° Download credential's json file. <br>
5° Install python3 dependencies and configure a local venv

<br>

![architecture](./images/architecture.png)


<br>
Refinements: Upgrade a local development to a docker container, then upload to a Google Cloud Platform using Cloud Run Service.

<br><br>

# Datalake - Cloud Storage

We're using cloud storage like a datalake, a object storage service. Extracting initial data on ANAC to datalake aren't automated. We are going to update the extract process on a second version.

![dtalake_cloud_storage](./images/datalake_cloud_storage.png)

<br>

# Transform - Python 

First, it's downloaded path structure of bucket according with storage and parameters of subfolders, that include from, report and year.

![datalake_folders](./images/datalake_folders.png)

Then, replicate path:

![path_local](./images/path_local.png)

Second, reading data in a local path according with folders and transform to a dataset.


<br>

# Datawarehouse - Bigquery

The last one step, saving processed data after all cleaning.

