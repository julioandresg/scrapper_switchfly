#PROJECT_ID = 'bc-te-dlake-dev-s7b3'
BUCKET = 'ltm-dlake-de-fti-ebz-us-dd6a5db6-ce80-4492-8b3a-c45f2ca86b62/Latam-Travel/Switchfly/'

#recordar que se instaló previamente
#pip install pandas
#pip install lxml
#pip install boto
#pip install gcs_oauth2_boto_plugin
#pip install --upgrade google-cloud-storage
#pip install xmltodict

import time
import os
import requests
from bs4 import BeautifulSoup
from bs4.element import Comment
import pandas as pd
import datetime
import xmltodict
from google.cloud import storage


def convertTuple(tup):
    str =  ''.join(tup)
    return str
#SE DEFINEN VARIABLES
today=datetime.datetime.now().strftime("%Y%m%d")
file="hist_switchfly_"+today+".csv"
outputFileName="hist_switchfly_"+today
finalFile=""
body=""
error=""
storage_client = storage.Client()
#SE DEFINEN VARIABLES DE DELTA T PARA BUSQUEDA DE INFORMACION
endDate = datetime.datetime.now().strptime(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),'%Y-%m-%dT%H:%M:%S')
strEndDate=datetime.datetime.strftime(endDate,"%Y-%m-%dT%H:%M")
t2 = datetime.timedelta(hours=2)
beginDate=endDate-t2
strBeginDate=datetime.datetime.strftime(beginDate,"%Y-%m-%dT%H:%M")
#SE REALIZA LA LLAMADA A LA API DE SWITCHFLY PARA OBTENER LOS ID's INGRESADOS EN LAS ULTIMAS 2 HORAS
url = "https://tamviagens.switchfly.com/api/api.cfm?xml=<PnrListRQ><UserId>10391433857</UserId><Password>Sampa2109</Password><BeginDate>",strBeginDate,"</BeginDate><EndDate>",strEndDate,"</EndDate><ModificationType></ModificationType><Unique>true</Unique></PnrListRQ>"
strUrl=convertTuple(url)
xml_data = requests.get(strUrl).content
soup = BeautifulSoup(xml_data, "xml")
service_id=soup.find_all('BookingId')
#POR CADA ID OBTENIDA SE IRÁ A BUSCAR SU XML CORRESPONDIENTE CON LA INFORMACION
for i in range(0, len(service_id)):
    id=service_id[i].get_text()
    #SEGUNDA LLAMADA A API DE SWITCHFLY CON LA CUAL SE OBTIENE LA INFORMACION DE LA ID CORRESPONDIENTE
    url_data='https://tamviagens.switchfly.com/api/api.cfm?xml=<ReadPnrRQ><UserId>10391433857</UserId><Password>Sampa2109</Password><RecordLocator>',id,'</RecordLocator><IncludeAccounting>True</IncludeAccounting></ReadPnrRQ>'
    strUrlData =convertTuple(url_data)
    xml_data2 = requests.get(strUrlData).content
    soup2 = BeautifulSoup(xml_data2, "xml")
    Pnr = str(soup2.find_all('Pnr'))
    x=Pnr.replace("[","")
    y=x.replace("]","")
    #SI POR ALGUN MOTIVO NO SE TIENE ACCESO AL XML OBTENEMOS EL ID DEL EXPEDIENTE ERRONEO
    try:
        my_dict=xmltodict.parse(y)
    except:
        error=error+id+"\n"
        print("ERROR AL LEER XML")
    ##SE OBTIENE LA DATA DE LOS NIVELES 1
    try:
        serviceId = (my_dict['Pnr']['RecordLocator'])
    except:
        serviceId=""
    try:
        clientTracking = (my_dict['Pnr']['ClientTracking'])
    except:
        clientTracking=""
    try:
        cobrand = (my_dict['Pnr']['Cobrand'])
    except:
        cobrand=""
    try:
        languageId=(my_dict['Pnr']['LanguageId'])
    except:
        languageId=""
    try:
        bookingTimestamp=(my_dict['Pnr']['BookingTimestamp'])
    except:
        bookingTimestamp=""
    try:
        status=(my_dict['Pnr']['Status'])
    except:
        status=""
    try:
        cancelDate=(my_dict['Pnr']['CancelDate'])
    except:
        cancelDate=""
    try:
        departureDate=(my_dict['Pnr']['DepartureDate'])
    except:
        departureDate=""
    try:
        departureAirPortCode=(my_dict['Pnr']['DepartureAirPortCode'])
    except:
        departureAirPortCode=""
    try:
        destinationAirPortCode=(my_dict['Pnr']['DestinationAirPortCode'])
    except:
        destinationAirPortCode=""
    try:
        subscribed=(my_dict['Pnr']['Subscribed'])
    except:
        subscribed=""
    try:
        certificates=(my_dict['Pnr']['Certificates'])
    except:
        certificates=""
    body=(body+str(serviceId)+"|"+str(clientTracking)+"|"+str(cobrand)+"|"+str(languageId)+"|"+str(bookingTimestamp)+"|"
          +str(status)+"|"+str(cancelDate)+"|"+str(departureDate)+"|"+str(departureAirPortCode)+"|"+str(destinationAirPortCode)
          +"|"+str(subscribed)+"|"+str(certificates)+"\n")
#SE GUARDA LA INFORMACION OBTENIDA EN 2 ARCHIVOS
#"archivo" CONTIENE LA INFORMACION DE LOS ID's OBTENIDOS Y SE ENCUENTRA LISTO PARA PROCESAR A BIGQUERY
#"archivo2" CONTIENE LOS ID's QUE POR ALGUN MOTIVO NO SE PUDO OBTENER EL XML
# exporting file to storage out
try:
    #print("Empieza a escribir archivo")
    tmp_folder = "/tmp/switchfly/"
    #tmp_folder = "C:/Users/zelot/Desktop/practicos GCP/"
    os.system("mkdir -p "+tmp_folder)
    archivo = open(tmp_folder+file, "w")
    archivo.write("serviceId | clientTracking | cobrand | languageId| bookingTimestamp| status | cancelDate| departureDate | departureAirPortCode | destinationAirPortCode | subscribed | certificates")
    archivo.write(body)
    archivo.close()
    archivo2 = open(tmp_folder+"errores.txt", "w")
    archivo2.write(error)
    archivo2.close()
    #print("termina de escribir archivo")
    # UNA VEZ CREADO LOS ARCHIVOS SE DETERMINA EL BUCKET AL QUE SE SUBIRÁ
    bucket = storage_client.get_bucket(BUCKET)
    # setea nombre archivo de salida y subida a storage
    blob = bucket.blob(file)
    blob.upload_from_filename(tmp_folder + file)
    blob2 = bucket.blob("errores.txt")
    blob2.upload_from_filename(tmp_folder + "errores.txt")
    #UNA VEZ SUBIDOS LOS ARCHIVOS, ESTOS SON ELIMINADOS DEL AMBIENTE VIRTUAL
    os.system("rm " + tmp_folder + "*")
except:
  print("No se pudó subir el archivo")

# generating output ZIP
'''
    # logging.info('Compressing '+output_filename)
    # with zipfile.ZipFile(tmp_folder+output_filename+".zip",'w', zipfile.ZIP_DEFLATED) as zip:
    #     zip.write(tmp_folder+output_filename+".txt",output_filename+".txt")
    #     zip.close()
    #     logging.info('Ending Compression...')

    # blob = bucket.blob(output_filename+".zip")
    # blob.upload_from_filename(tmp_folder+output_filename+".zip")
'''