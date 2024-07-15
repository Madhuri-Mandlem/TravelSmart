Travel Smart collects real-time traffic data, including vehicle flow, congestion, and accidents.
Weather Information: Provides weather forecasts, temperature, and other meteorological data.
Surveillance Cameras: Capture video feeds from different locations.
Emergency Services: Gather information related to emergencies and incidents.

Techs: Kafka Streaming Platform, Zookeeper, Data Processing with Apache Spark, Docker.

Data Flow:
Data from various sources is captured and sent to Kafka.
ZooKeeper ensures the reliable operation of Kafka.
Spark processes data streams from Kafka, performing transformations and aggregations.
Data is stored in AWS S3 as raw and transformed data.
AWS Glue performs data cleaning and transformation tasks.
Data is loaded into AWS Redshift for analysis.
Power BI, Tableau, and Looker Studio visualize and analyze data from Redshift, providing insights for decision-making.

