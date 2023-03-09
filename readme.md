# kinesis-http-python #

## Code to act as an endpoint for kinesis http delivery ##
This is an example server on how to retrevive the data from the aws firehose and process it for future use.
This is a basic example only showing how to retreive the data and process it.

The server needs a https server running for it to get the data from firehose.

## Setup ##
``` pip install -r requirements 
    python application.py
```

## Testing ## 

To test its working send the example request provided to the local endpoint with a post request /ingest
The example payload has 12058 records while normal payloads have around 120 records

```curl --location 'http://192.168.1.77:8080/ingest' --header 'Content-Type: application/gzip' --data '@payload-avro-format.json.gz'```