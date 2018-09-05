## Databricks Remote Execution (Python 2.7)

This is a lambda package to run remote commands on Databricks


Goal: Run remote commands (python / scala / sql) on Databricks via AWS Lambda Functions

Lambda Requirements:
* Load all dependencies into a zip file
* Load the code base into the same zip file


Loop over the requirements.txt file to load the dependencies into a `dep/` folder:  
```
#!/bin/bash

rm -rf ./dep/
mkdir -p dep/
for i in `cat requirements.txt`;
do
  echo "Downloading dep: $i ..."
  pip install --install-option="--prefix=" $i -t ./dep/
done
```

## Building the Lambda Package
1. Run the following commands to download python dependencies and build a zip of the project
```
$ chmod +x download_deps.sh rebuild.sh
$ ./download_deps.sh
$ ./rebuild.sh
```
2. Upload the zip to the AWS Lambda Operator
