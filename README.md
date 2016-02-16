# Smart Meter Watchdog project in Insight Data Engineer program
(ongoing)

## Introduction
Smarter Meter Online is a "hypothetical" system to monitor home energy consumption. The idea of the project is to attach power sensor to all the major appliances around the home, and the system can monitor/alert energy consumption in details. It takes in real time data feeding (engineered), and builds data pipeline to generate home energy usage dashboard and detailed appliance energy usage. In addition, it also detects sudden spike in the power reading from any meter.

## Data Sources
The raw data is [The Reference Energy Disaggregation Data Set](http://redd.csail.mit.edu/) from MIT. It has 6 houses available for download, and has a range of ~30 days each. This project uses it as source data and generate more houses featured random spike and scaling. The original data looks like the following (Basicall each record contains (timestamp, powerReading) pair):

![raw_source_data](images/data_source.png)

In the batch layer of processing, there are 200GB data being generated with these data sources and sent to the data pipeline. It contains about 600 million records in total.

## Data pipeline:
The data pipeline is shown as the following figure:

![data_pipeline](images/data_pipeline.png)

I choose Kafka because it is fault tolerant and is able to buffer the data for some time and can support different types of consumers. Camus is used as the tool to read the data from Kafka and write to HDFS. The data processing part contains two portions, one is the batch layer processing, which contains computing the daily energy consumption for each appliance as well as the main power. It also uses hivecontext to compute the 97% percentile for each meter and use in the real time layer. 
