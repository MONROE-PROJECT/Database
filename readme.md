# MONROE: Database Structure
This database is setup for the MONROE project and all files are under the GNU (GENERAL PUBLIC LICENSE, version 3) license.

# Folder Structure
* /tables -- Each .cql describes one table
    * The .cql where created by executing ```describe table X;``` and not by the commands they where created with, ie default values are expressed in the .cql files.

* db_schema.cql

# Quick howto
In order to create the DB schema (and *drop* the data it contains), you can use the following command:

./cqlsh -f db_schema.cql

Please note the **drop** command on the first line.

!!! @Rafael: could you please update the configuration details? !!!
