# Data Quality Assessment

## Project Abstract

In order to check the reliability of large-scale datasets and set realistic goals for cleaning data, data quality assessment must be completed as a preliminary step.

In this study, we designed a comprehensive pipeline for data quality assessment. The selection of datasets from NYC Open Data is performed first, followed by using the YData-Profiling tools to profile them to get an overview of data properties. After manually inspecting in the obtained metadata for identifying any patterns or anomalies for errors, the main stage of coding PySpark scripts is performed. We classify errors into null values, misspellings/abbreviations, and invalid values including outliers, pattern violations, and constraint violations, implementing various detection functions for each.

We also use precision and recall metrics to evaluate our approach and explore the limitation in identifying the semantics of column names. Overall, our study provides a semi-automated technique for data quality assessment, addressing the challenges of error detection in large-scale datasets.

## Main Pipeline

![Pipeline](/img/flowchart.drawio.png)

## Quickstart

### Install Apache-Spark

```shell
brew install apache-spark
```

### Install Requirements for Python

```shell
pip install pyspark
pip install jellyfish
pip install ydata_profiling
```

### Download Datasets for Testing

Dowload the following datasets and pu them in the `dataset` folder.

[Construction Demolition Registrants](https://data.cityofnewyork.us/City-Government/Construction-Demolition-Registrants/cspg-yi7g)

[DOHMH New York City Restaurant Inspection Results](https://data.cityofnewyork.us/Health/DOHMH-New-York-City-Restaurant-Inspection-Results/43nn-pn8j)

[Civil Service List (Active)](https://data.cityofnewyork.us/City-Government/Civil-Service-List-Active-/vx8i-nprf)

[2013 - 2018 Demographic Snapshot School](https://data.cityofnewyork.us/Education/2013-2018-Demographic-Snapshot-School/s52a-8aq6)

[2010 - 2016 School Safety Report](https://data.cityofnewyork.us/Education/2010-2016-School-Safety-Report/qybk-bjjc)

## Team Member

Xinsheng Zhu

Ying Li

Can Wang
