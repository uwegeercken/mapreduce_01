Hadoop mapreduce job using the JaRE business rule engine.

The idea here is to use the business rule engine to filter the data. because the ruleengine
rules (business logic) are maintained external to the code, changes to the logic won't require
changes to the mapreduce code!

A web application is available to create and maintain the complex business logic in an easy fashion.
Once complete, the logic can be exported to a single file that is used by the ruleengine.

The job is an example job to read rows of csv data, filter the data in the mapping phase using
the ruleengine. If the data passes the business logic, create a key and value and pass it to the
reduce phase.

In the latest version data from geonames.org is used. Download the allCountries.txt file, if you want
to run this example mapreduce job. The mapreduce_countries.sh script can be used to run the job, but
make sure to adjust the path and other information as required in the script.

Instead of filtering data one could also modify the data before it runs through mapreduce.
Or of course also both: filtering and modifying.

The key for the mapping phase can be built from multiple fields/columns from the incoming data
row and is defined in a properties file. The field/column to use for the value as well.


    Copyright (C) 2008-2017  Uwe Geercken

 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

last update: uwe geercken 2017-03-04
