import pydicom
from pydicom.misc import is_dicom
import json
from lxml import etree
import psycopg2
import psycopg2.extras
import queue
import threading
import time

#import dicom

'''
Helper Methods
'''

#Global variables to handle thread communications
global_fifo_q = queue.Queue()

class PGConnectDB:
    def __init__(self, dsn_hostname, dsn_port, dsn_database, dsn_uid, dsn_pwd):
        self.dsn_hostname = dsn_hostname
        self.dsn_port = dsn_port
        self.dsn_database = dsn_database
        self.dsn_uid = dsn_uid
        self.dsn_pwd = dsn_pwd

        try:
            conn_string = "host=" + self.dsn_hostname + " port=" + self.dsn_port + " dbname=" + self.dsn_database + " user=" + self.dsn_uid + " password=" + self.dsn_pwd
            print("Connecting to database\n  ->%s" % (conn_string))
            self.conn = psycopg2.connect(conn_string)
            print("Connected!\n")
        except:
            print("Unable to connect to the database.")

class StoreData(PGConnectDB):
    pass
    def retrieve_and_store(self):
        global global_fifo_q
        print ('In Store Data thread')
        print (self.conn)
        while True:
            if not global_fifo_q.empty():
                data_from_q = global_fifo_q.get()
                print (data_from_q)



# Given a string, converts it to lower camel case
def lowerCamelCase(input):
    camel = ''.join(x for x in input.title() if (not x.isspace() and x.isalnum()))
    result = camel[0].lower() + camel[1:]
    return result


# Normalize weight values into preferred units (kg)
def normalizeWeightValue(value, units):
    # default = kg
    result = float(value)
    # if units.strip() == "lbs":
    #     # convert from lbs to kg
    #     result = float(value) / (2.20462)
    return result


# Normalize percentage values into preferred values [0, 1]
def normalizePercentageValue(value):
    result = float(value) / float(100)
    return result


'''
JSON Parse Execution
'''
# Change to os.walk path
#dcm_file = pydicom.read_file('c:/temp/pending_dexafit_files/1.2.840.113619.2.110.500342.20180111131347.3.1.12.1.dcm')

if is_dicom('c:/temp/pending_dexafit_files/1.2.840.113619.2.110.500342.20180111131347.3.1.12.1.dcm'):
    print("It is indeed DICOM!")
    dcm_file = pydicom.dcmread('c:/temp/pending_dexafit_files/1.2.840.113619.2.110.500342.20180111131347.3.1.12.1.dcm')
else:
    print("It's probably not DICOM")


parsed_result = {}
testType = dcm_file.StudyDescription
studyDate = dcm_file.StudyDate
studyTime = dcm_file.StudyTime
deviceSerialNumber = dcm_file.DeviceSerialNumber
institutionName = dcm_file.InstitutionName
manufacturer = dcm_file.Manufacturer
#manufacturerModelName = dcm_file.ManufacturersModelName
#entranceDose = dcm_file.EntranceDoseinmGy
studyInstanceUID = dcm_file.StudyInstanceUID
seriesInstanceUID = dcm_file.SeriesInstanceUID
parsed_result["testInfo"] = {
    "testType": testType,
    "studyDate": studyDate,
    "studyTime": studyTime,
    "deviceSerialNumber": deviceSerialNumber,
    "institutuionName": institutionName,
    #"manufacturerModelName": manufacturerModelName,
    "manufacturer": manufacturer,
    #"entranceDoseinmGy": entranceDose,
    "studyInstanceUID": studyInstanceUID,
    "seriesInstanceUID": seriesInstanceUID
}
#user_firstName = dcm_file.PatientsName.given_name
#user_lastName = dcm_file.PatientsName.family_name
user_id = dcm_file.PatientID
ethnic_group = dcm_file.EthnicGroup
#user_birthdate = dcm_file.PatientsBirthDate
#user_sex = dcm_file.PatientsSex
#user_age = dcm_file.PatientsAge
#user_Size = dcm_file.PatientsSize
#user_Weight = dcm_file.PatientsWeight
parsed_result["userInfo"] = {
    #"firstName": user_firstName,
    #"lastName": user_lastName,
    "email": user_id,
    "ethnicGroup": ethnic_group,
    #"birthDate": user_birthdate,
    #"userSex": user_sex,
    #"userAge": user_age,
    #"userSize": user_Size,
    #"userWeight": user_Weight
}


xml_string = dcm_file.ImageComments
xml_root = etree.fromstring(xml_string)

parsed_result["bodyComposition"] = {}
for leaf in xml_root.iter('COMP_ROI'):
    regionName = lowerCamelCase(leaf.attrib['region'])
    parsed_result["bodyComposition"][regionName] = {}
    for reading in leaf.iter():
        # skip attributes that don't have a value
        if not 'units' in reading.attrib:
            continue
        # Normalize the value (% or lbs)
        key = lowerCamelCase(reading.tag)
        units = reading.attrib['units'].strip()
        value = None
        if units == '%':
            value = normalizePercentageValue(float(reading.text))
        else:
            value = normalizeWeightValue(float(reading.text), units)
        # save the reading
        parsed_result["bodyComposition"][regionName][key] = value


parsed_result["BMD"] = {}
for leaf in xml_root.iter('ROI'):
    regionName = lowerCamelCase(leaf.attrib['region'])
    parsed_result["BMD"][regionName] = {}
    for reading in leaf.iter():
        if reading.text is None:
            continue
        elif reading.text is '-':
            continue
        key = lowerCamelCase(reading.tag)
        # units = reading.attrib['units'].strip()
        value = float(reading.text)
        parsed_result["BMD"][regionName][key] = value

parsed_result["visceralFat"] = {}
for leaf in xml_root.iter('VAT_MASS'):
    regionName = lowerCamelCase('Estimated Visceral Adipose Tissue')
    parsed_result["visceralFat"][regionName] = {}
    for reading in leaf.iter():
        # skip attributes that don't have a value
        if not 'units' in reading.attrib:
            continue
        # Normalize the value (% or lbs)
        key = lowerCamelCase(reading.tag)
        units = reading.attrib['units'].strip()
        value = None
        if units == '%':
            value = normalizePercentageValue(float(reading.text))
        else:
            value = normalizeWeightValue(float(reading.text), units)
        # save the reading
        parsed_result["visceralFat"][regionName][key] = value

# convert it all to JSON and Save
json_result = json.dumps(parsed_result)
print(json_result)
# When ready to save as JSON file.
# with open('DXAParse.json','w') as outfile:
#     outfile.write(json_result)

# DICOM File has been parsed to JSON and saved as a new file.

'''
Add to database - Execution
'''
# Database Connection Details
dsn_database = "dexafitpostgres"            # e.g. "compose"
dsn_hostname = "dexafit-postgres-instance.cnhfhogvgcm2.us-east-2.rds.amazonaws.com" # e.g.: "aws-us-east-1-portal.4.dblayer.com"
dsn_port = "5432"                 # e.g. 11101
dsn_uid = "aws_dexafit"        # e.g. "admin"
dsn_pwd = "$$dicomaws$$"      # e.g. "xxx"





try:
    conn_string = "host="+dsn_hostname+" port="+dsn_port+" dbname="+dsn_database+" user="+dsn_uid+" password="+dsn_pwd
    print ("Connecting to database\n  ->%s" % (conn_string))
    conn=psycopg2.connect(conn_string)
    print ("Connected!\n")
except:
    print ("Unable to connect to the database.")



cursor = conn.cursor()

psycopg2.extras.register_uuid()


sqlselect = "select userid from dexafit.userinfo where dexafitpatientid = %s;"
dexafitUUID = (user_id, )
cursor.execute(sqlselect, dexafitUUID)
print("successfully selected records")
uuid = cursor.fetchone()[0]
print(uuid)


# # insert with uuid and json

sqlinsert = "INSERT into dxa.dxatest_sudip (userid, testdate, testtime, results) VALUES(%s, %s, %s, %s);"
dexa = (uuid, studyDate, studyTime, json_result)
cursor.execute(sqlinsert, dexa)
print("Successfully inserted records")

conn.commit()

conn.close()


global_fifo_q.put(json_result)
data_from_q = global_fifo_q.get()
print (data_from_q)

def Main():



if __name__ == '__main__':
    Main()


