# Importer
Very simple data importer that inserts json objects into a Cassandra db.

All files in, the specified directory and all subdirectories excluding
failed and done directories, containing
json objects are read and parsed into CQL (Cassandra) statements.

Note: A json object must end with a newline \n (and should for performance
 be on a single line).
 The program is (read tries to be) designed after http://tinyurl.com/q82wtpc

File extensions allowed : .json and .xz

# Usage
Usage :
export MONROE_DB_USER=<user>; export MONROE_DB_PASSWD=<password>; python monroe_dbimporter.py --indir=<input directory of source files> --failed=<output of failed files> --processed=<output of succeded inserts> --authenv  --host=<hostname or ip> --keyspace=<keyspace> --interval=<seconds>  --verbosity=[0,1,2] --concurrency=<number of processes>

# Dependencies
python-lzma
python-cassandra
