from ast import parse
import logging
import azure.functions as func 
from azure.servicebus import ServiceBusClient, ServiceBusMessage, TransportType
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__ 
import json 
import hl7tojson.parser
import hl7
import datetime
import os

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()            
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        logging.info('calling read_msg_from_sb .')
        read_msg_from_sb()
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )


#Create the BlobServiceClient object which will be used to create a container client
blob_conn_str = "DefaultEndpointsProtocol=https;AccountName=teststoragetest33;AccountKey=CMEojx+VUKedJsJzw85TjDbZTRaAnZ3E6pYvW1kyUnTYIFg1bHhzlsCcP65wjtSEBpbwev5yljr6+AStX8gQNA==;EndpointSuffix=core.windows.net";
blob_service_client = BlobServiceClient.from_connection_string(blob_conn_str)
AzureWebJobsStorage = "DefaultEndpointsProtocol=https;AccountName=teststoragetest33;AccountKey=CMEojx+VUKedJsJzw85TjDbZTRaAnZ3E6pYvW1kyUnTYIFg1bHhzlsCcP65wjtSEBpbwev5yljr6+AStX8gQNA==;EndpointSuffix=core.windows.net";

 


"""
Routes and views for the flask application.
"""
#read message from service bus-queue
def read_msg_from_sb():
    starttime = datetime.datetime.now()
    try:
      CONNECTION_STR = "Endpoint=sb://servicebus55test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=h88zc5c1q5MWsCliO/R7aZ1KqqgEf94aihMXGfFoMMY="
      QUEUE_NAME = "hl7-queue" 
      servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR, logging_enable=True,transport_type=TransportType.AmqpOverWebsocket)
      with servicebus_client:
        receiver = servicebus_client.get_queue_receiver(queue_name=QUEUE_NAME, max_wait_time=5)
        with receiver:
            for msg in receiver:
                logging.info("=========received msg from service bus===========")
                logging.info("Received: " + str(msg))
                fileinfo = str(msg)
                y = json.loads(fileinfo)
                logging.info("Process started ,filename:"+ y["FileName"])
                container_name = 'raw'
                logging.info("=========read data from blob storage===========")
                #read blob data                
                blob_client = blob_service_client.get_blob_client(container = container_name, blob=y["FileName"])
                blob_data = blob_client.download_blob()
                data = blob_data.readall()
                #validate data
                if data != False :
                   logging.info("=========read data from blob storage completed===========")
                   logging.info("=========send data to hl7 praser ===========")
                   logging.info("=========decode data started ===========")
                   data = data.decode("utf-8")
                   logging.info("=========decode data completed ===========")
                   validationresult = hl7parser(y["FileName"] , data)
                   if validationresult:
                       logging.info("=========hl7 parser validation success ,move file from raw to hl7raw storage ===========")
                       #after validation success send file from raw to  hl7raw container  
                       #copied_blob = blob_service_client.get_blob_client(container = "hl7raw", blob = y["FileName"])
                       #copy = copied_blob.start_copy_from_url(blob_client.url)
                       #props = copied_blob.get_blob_properties()
                       #logging.info(props.copy.status)
                       #logging.info("=========File moved success  from raw to hl7raw storage ===========")
                       logging.info("Remove file info in service bus started")
                       #remove file info in service bus 
                       receiver.complete_message(msg) 
                       logging.info("Remove file info in service bus completed")
        endtime = datetime.datetime.now()
        logging.info(starttime)
        logging.info(endtime)
        logging.error("No msg in queue to process")
    except Exception as ex:
      logging.error("Something else went wrong" + ex)


#read message from blob storage
def read_data_from_hl7_blob(container_name,local_file_name):
    try: 
        #Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name) 

        #download blob
        blob_data = blob_client.download_blob()
        data = blob_data.readall()
        return data; 
    except Exception as ex:
      print(ex)
      return False

#hl7 parser operation
def hl7parser(filename , message): 
    try:      
        #parse hl7 message 
        print("================= hl7.parse started ==========================")  
        print(message)
        #h = hl7.parse(message)  
        h = hl7tojson.parser.parse(message)
        logging.info('filename before' + os.path.splitext(filename)[0])
        fn = os.path.splitext(filename)[0]
        logging.info('filename after' + fn)
        if (uploadfiletoblob('hl7jsonfunapp', fn + '.json', h)):
            logging.info(h)        
            print(h) 
            print(str(h) == message)
            print(type(h)) 
            logging.info('jsonfile created success' + fn)
            return True
        else:
            logging.info('fail to create json' + fn)
            return False
    except Exception as ex:
      h = hl7.parse(message)         
      logging.info(ex)
      return False

 

#blobcopy source to dest
def blobcopy(sourceboburl,container,filename):
    try:      
        copied_blob = blob_service_client.get_blob_client(container = container, blob = filename)
        copy = copied_blob.start_copy_from_url(sourceboburl)
        props = copied_blob.get_blob_properties()
        print(props.copy.status)
        return True
    except Exception as ex:
      print(ex) 
      return False
    

#create new file in blob
def uploadfiletoblob(containername, filename, data): 
    logging.info('uploading file to ' + containername)
    try:       
        # Upload the created file  
        y = json.dumps(data)
        blob_client = blob_service_client.get_blob_client(container = containername, blob = filename)  
        blob_client.upload_blob(y)
        props = blob_client.get_blob_properties()
        print(props.copy.status)
        logging.info('uploading file completed ')
        return True
    except Exception as ex:
      logging.error('create json blob failed' + ex)
      print(ex) 
      return False  

#read_msg_from_sb()

#uploadfiletoblob("hl7jsonfunapp", "test.json", "hello")




 