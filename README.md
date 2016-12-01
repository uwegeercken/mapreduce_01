# mapreduce_01
mapreduce job using the JaRE business rule engine

the idea here is to use the business rule engine to filter the data. because
the ruleengine rules (business logic) is maintained external to the code, changes
to the logic won't require changes to the code!

the job is an example job to read rows of csv data, create a key from a config file,
filter the data with the rule engine and sum up values for a defined column.

Instead of filtering data one could also modify the data before it runs through mapreduce,
or of course also both: filtering and modifying.

last update: uwe geercken 2016-12-01
