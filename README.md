Hadoop mapreduce job using the JaRE business rule engine.

The idea here is to use the business rule engine to filter the data. because the ruleengine
rules (business logic) are maintained external to the code, changes to the logic won't require
changes to the mapreduce code!

A web application is available to create and maintain the complex business logic in an easy fashion.
Once complete, the logic can be exported to a single file that is used by the ruleengine.

The job is an example job to read rows of csv data, filter the data in the mapping phase using
the ruleengine. If the data passes the business logic, create a key and value and pass it to the
reduce phase.

Instead of filtering data one could also modify the data before it runs through mapreduce.
Or of course also both: filtering and modifying.

The key for the mapping phase can be built from multiple fields/columns from the incoming data
row and is defined in a properties file. The field/column to use for the value as well.

last update: uwe geercken 2016-12-01
