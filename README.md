# mapreduce_01
mapreduce job using the JaRE business rule engine

The idea here is to use the business rule engine to filter the data. because
the ruleengine rules (business logic) are maintained external to the code, changes
to the logic won't require changes to the code!

The job is an example job to read rows of csv data, filter the data in the mapping phase using
the ruleengine. If the data passes the business logic, create a key and value and pass it to the
reduce phase.

Instead of filtering data one could also modify the data before it runs through mapreduce.
or of course also both: filtering and modifying.

last update: uwe geercken 2016-12-01
