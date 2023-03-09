import base64
import gzip
import json
import snappy
import avro.schema
from avro import schema
from avro.io import BinaryDecoder, DatumReader
from flask import Flask, request, make_response
from OpenSSL import SSL
from io import BytesIO
import logging
import concurrent.futures

application = Flask(__name__)

logging.basicConfig(level=logging.INFO)

elbPath = 'SabreSearch.avsc'

# Pre-load the Avro schema from the file
with open(elbPath, 'r') as f:
    schema_string = f.read() 
    sabreSearch = avro.schema.Parse(schema_string)

# Pre-load the Snappy decompression library
snappy_preload = snappy.uncompress(b'\x00')

def create_avro_object(record):
        #Gets the current data from the record
        data = record['data']

        #Decodes the data from base64 to gzip
        decoded_data = base64.b64decode(data)
        decoded_data = decoded_data.rstrip(b'\n') # remove newline marker

        # uncompress the record bytes using Snappy
        data_bytes = snappy.uncompress(decoded_data)

        #Parse the data and map it to the avro schema to make the python objects
        reader = DatumReader(sabreSearch)
        binary_decoder = BinaryDecoder(BytesIO(data_bytes))
        avro_object = reader.read(binary_decoder)
        return avro_object

def uncompressRequest(request_data):
    #Uncompresses the decoded_data
    ungziped_data = gzip.decompress(request_data)

    # load the resulting string as a JSON object
    return json.loads(ungziped_data.decode('utf-8').lstrip('\n'))

def createAvroObjects(records):
    avro_objects = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        avro_objects = list(executor.map(create_avro_object, records))

    return avro_objects
    

@application.route('/')
def index():
    return '<html><body><h1>Python sabre streaming ingest example</h1></body></html>'

@application.route('/ingest', methods=['POST'])
def handle_firehose_message():
    #Load data from request
    request_data = request.data
    
    #Set response saying data was received
    response_text = "data sucessfully received"

    #Use on_response_close to run the mapping and decompression after the response has been sent
    def on_response_close():
        logging.info("Uncompressing request")

        #Uncompress the data
        data_dict = uncompressRequest(request_data)

        #Set the requestID from the payload
        requestId = data_dict['requestId']
        #Set the timestamp from the payload 
        timestamp = data_dict['timestamp']

        #Set the records from the payload
        records = data_dict['records']
        
        logging.info("Started the mapping of avroobjects")
        #Extract Records and map them to the avrio object SabreSearch.avsc (Schema found in the file SabreSearch.avsc)
        avroObjects = createAvroObjects(data_dict['records'])

        logging.info(f"Request ID: {requestId}, Timestamp: {timestamp}, Number of records pre decompression and extraction: {len(records)}, Number of converted records {len(avroObjects)}")

    #Set the response text
    response = make_response(response_text)

    #Set the call_on_close to run the data mapping
    response.call_on_close(on_response_close)
    #Return the response
    return response

if __name__ == '__main__':
    application.run("0.0.0.0", "8080")
