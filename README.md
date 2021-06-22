# Dataflow Bigquery Schema Migrator Insert
This pipeline accepts JSON from Cloud PubSub, dynamically redirects that JSON Object based on a predefined key
to a target BigQuery table, an attempt at inserting the data is made,
if this fails data is gathered into window of n configurable minutes, the data in this window is then keyed by target table and the incoming schema changes for each table are merged,
this prevents multiple schema changes to bigquery every second, then
the target table's schema is adjusted to accomodate the incoming JSON Object such that, it is able to be inserted,
the failed data is sent back to the top of pubsub so it can be inserted via the process mentioned earlier.

![](docs/SchemaMigrator.png?raw=true)

Please bear in mind mulitple tables can be updated all at once, do not set the window value too low.

Example Dataflow pipeline:

![](docs/SchemaMigratorDAG.png?raw=true)

## Limitations 

* Currently only a flat JSON Object can be accepted, this will change in future releases. This means no JSON arrays or nested JSON objects can be accepted as keys in the JSON object. This is due to polymorphic JSON issues, not interacting well with BigQuery, the full list of acceptable types are as follows:
  - Numeric Types: Integer and Longs
  - Bytes: Byte and Byte[]
  - Floating Point Types: Double and Float
  - Limited Support for ISO datetime: following format only "yyyy-MM-dd HH:mm:ss" 

Please see [TableRowToSchema.java](src/main/java/com/doit/schemamigration/Parsers/TableRowToSchema.java), for more information.

Example acceptable object:
```
{"test":"test","counter":3}
```
Example for an unacceptable object, with a JSON array:
```
{"test":[1,2,3]}
```
Example for an unacceptable object, with a nested JSON object:
```
{"test":{"test":3}}
```

* Infinite retries, there is no limit on the number of retries. incoming JSON could get stuck forever, in the retry loop if there is an issue, in future releases all data will have a set number of retries.

* Still must use correct column and table names for Big Query see [here for column names](https://cloud.google.com/bigquery/docs/schemas#column_names) and [here for table names](https://cloud.google.com/bigquery/docs/tables#table_naming). No sanitation or validity checks of the table or columns names will be performed, bad data will end in bad data table. 

## Requirements

* Java 11
* Enabled Google Bigquery API
* Enabled Google PubSub
* Create a topic and subscriber in Google Pubsub with the same name
* Permissions on both Google Bigquery and Pubsub, recommend: set GOOGLE_APPLICATION_CREDENTIALS environment variable

## How to run me

```bash
./gradlew clean shadowJar \
  && java -jar build/libs/schemamigration-1.0.0.jar \ --runner=DataflowRunner \ 
  --inputTopic=<topic and subscriber to listen to they are assumed to have the same name> \ 
  --jsonAttributeForTargetTableName=<name of table in json object>\
  --failOverTableName=<full path to failure table> \
  --targetDataset=<your dataset> \
  --project=<project name>  \
  --tempLocation=gs://<bucket-name>/staging
```

## TODO

* Accept more complex schema
* MOAR tests need 100%
