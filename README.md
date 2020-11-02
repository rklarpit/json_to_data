# JSON to Data
## Problem
The product management team have agreed to ingest this new payload. The desire is to have all the data stored and queryable via our business intelligence tool.

## Solution
There are 4 folders, organized in an ideal programming scenario.
1. Handler will consist of the serverless function that will execute when the API is triggered
2. Services folder has the service file which will have various python functions aiding in creating and loading data into newer tables(if already not created)
3. Tools folder has the DBtools class that will be used to connect to a given database and execute queries against either
4. Others It has a file consisting of few BI specific dynamic queries
5. I have provided a jupyter notebook which can elaborate about the various functions used in the services file.
6. template yaml is a pseudo version of the template to be used for the framework. Note it is a version for SAM template and not serverless

The sample queries would need deeper understanding of the relation of tables, how to establish them.
There could be alot of BI done on the given problem but to limit myself to the time provided I have provided 2 simple SQL queried for 2 problems I could think of

# Other parts of Readme as per the problem set:-
## Deliverables
* Automated database table creation based on [JSON file](https://github.com/great-jones/json_to_data/blob/main/sample.json)
* Interpret datatypes based on each attributes in [JSON file](https://github.com/great-jones/json_to_data/blob/main/sample.json)
* Create new tables if the payload changes
* Sample queries that BI team can use to build their analytics

## Requirements
* Any programming language may be used
* Database engine is Postgres v11.8 or newer
* Utilize [Serverless Framework](https://www.serverless.com/) (Nice to have, not required)

## Timeline and Objective
This evaluation is intended to evaluate what you can accomplish within a two hour timeframe. The goal is to get a sense on how you would achieve the desired deliverables in the given amount of time. If you do not complete the solution, this does not indicate failure.
### Recommendations
* **Have Fun**
* Create a ReadMe specification
* Provide comments where necessary
* **Have Fun**

## Completion
* Clone this branch to a repository that is accessible to Great Jones
* When you have either elapsed two hours or have a complete solution, whichever comes first, Stop!
* Email your contact from Great Jones that your solution is ready and where the solution can be obtained.