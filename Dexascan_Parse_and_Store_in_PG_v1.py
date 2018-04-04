import pydicom
from pydicom.misc import is_dicom
#https://github.com/pydicom/pydicom/wiki/Porting-to-pydicom-1.0
import json
from lxml import etree
import psycopg2
import psycopg2.extras
import queue
import time
import glob, os, shutil
from PGConnectDB import *


# Global variables to handle thread communications
global_fifo_q = queue.Queue()


class StoreData(PGConnectDB):
    pass

    def retrieve_and_store(self, incomplete_directory, processed_directory):
        self.incomplete_directory = incomplete_directory
        self.processed_directory = processed_directory
        global global_fifo_q
        print('In Store Data thread')
        print(self.conn)
        while True:
            if not global_fifo_q.empty():
                #Get data from FIFO queue
                data_from_q = global_fifo_q.get()

                self.file = data_from_q[4]

                print("Printing data in queue prior to storage..")
                #print(data_from_q[0])
                #print(data_from_q[1])

                # Select UUID from dexafit.userinfo
                self.cursor = self.conn.cursor()
                psycopg2.extras.register_uuid() # What is this for ?

                # Error handing needed for SQL and connection.
                sqlselect = "select userid from dexafit.userinfo where dexafitpatientid = %s;"
                dexafitUUID = (data_from_q[0],)
                self.cursor.execute(sqlselect, dexafitUUID)
                print("successfully selected records")
                self.uuid = self.cursor.fetchone()
                print(self.uuid)

                if (self.uuid):
                    # insert with uuid and json
                    self.json_result = data_from_q[1]
                    self.studyDate = data_from_q[2]
                    self.studyTime = data_from_q[3]

                    # Error handing needed for SQL and connection.
                    self.sqlinsert = "INSERT into dxa.dxatest_sudip (userid, testdate, testtime, results) VALUES(%s, %s, %s, %s);"
                    self.dexa = (self.uuid, self.studyDate, self.studyTime, self.json_result)
                    self.cursor.execute(self.sqlinsert, self.dexa)
                    print("Successfully inserted records")

                    #Move Dicom File
                    print ("Trying to move DICOM file to:"+self.processed_directory)
                    #source = 'c:/temp/pending_dexafit_files/1.2.840.113619.2.110.500342.20180111131347.3.1.12.1.dcm'
                    #destination = 'c:/temp/processed_dexafit_files/1.2.840.113619.2.110.500342.20180111131347.3.1.12.1.dcm'
                    source = self.file
                    #destination = 'c:/temp/processed_dexafit_files/'
                    destination =self.processed_directory

                    try:
                        shutil.move(source, destination)
                        self.conn.commit()
                    except shutil.Error as e:
                        print('Error: %s' % e)
                        self.conn.rollback()
                        os.remove(self.file)
                        # eg. source or destination doesn't exist
                    except IOError as e:
                        print('Error: %s' % e.strerror)
                        self.conn.rollback()
                        os.remove(self.file)
                else:
                    print ("Missing UUID")
                    print("Trying to DICOM file to:"+self.incomplete_directory)
                    source = self.file
                    destination = self.incomplete_directory
                    try:
                        shutil.move(source, destination)
                        self.conn.rollback()
                    except shutil.Error as e:
                        print('Error: %s' % e)
                        self.conn.rollback()
                        os.remove(self.file)
                        # eg. source or destination doesn't exist
                    except IOError as e:
                        print('Error: %s' % e.strerror)
                        self.conn.rollback()
                        os.remove(self.file)


class ReadDICOMFiles:
    def __init__(self, source_directory, error_directory):
        self.source_directory = source_directory
        self.error_directory = error_directory
        print ('In ReadDICOMFiles...')

    def retrieve(self):
        global global_fifo_q
        os.chdir(self.source_directory)
        while True:
            time.sleep(10)
            for file in glob.glob("*.dcm"):
                print(file)
                if is_dicom(file):
                    print("It is indeed DICOM!")
                    dcm_file = pydicom.dcmread(file)
                    print(dcm_file)

                    parsed_result = {}
                    testType = dcm_file.StudyDescription
                    studyDate = dcm_file.StudyDate
                    studyTime = dcm_file.StudyTime
                    deviceSerialNumber = dcm_file.DeviceSerialNumber
                    institutionName = dcm_file.InstitutionName
                    manufacturer = dcm_file.Manufacturer
                    # manufacturerModelName = dcm_file.ManufacturersModelName
                    # entranceDose = dcm_file.EntranceDoseinmGy
                    studyInstanceUID = dcm_file.StudyInstanceUID
                    seriesInstanceUID = dcm_file.SeriesInstanceUID
                    parsed_result["testInfo"] = {
                        "testType": testType,
                        "studyDate": studyDate,
                        "studyTime": studyTime,
                        "deviceSerialNumber": deviceSerialNumber,
                        "institutuionName": institutionName,
                        # "manufacturerModelName": manufacturerModelName,
                        "manufacturer": manufacturer,
                        # "entranceDoseinmGy": entranceDose,
                        "studyInstanceUID": studyInstanceUID,
                        "seriesInstanceUID": seriesInstanceUID
                    }
                    # user_firstName = dcm_file.PatientsName.given_name
                    # user_lastName = dcm_file.PatientsName.family_name
                    user_id = dcm_file.PatientID
                    ethnic_group = dcm_file.EthnicGroup
                    # user_birthdate = dcm_file.PatientsBirthDate
                    # user_sex = dcm_file.PatientsSex
                    # user_age = dcm_file.PatientsAge
                    # user_Size = dcm_file.PatientsSize
                    # user_Weight = dcm_file.PatientsWeight
                    parsed_result["userInfo"] = {
                        # "firstName": user_firstName,
                        # "lastName": user_lastName,
                        "email": user_id,
                        "ethnicGroup": ethnic_group,
                        # "birthDate": user_birthdate,
                        # "userSex": user_sex,
                        # "userAge": user_age,
                        # "userSize": user_Size,
                        # "userWeight": user_Weight
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
                            # value = float(reading.text)
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

                    # Add data to queue
                    data_to_process = (user_id, json_result, studyDate, studyTime, file)
                    global_fifo_q.put(data_to_process)

                else:
                    print("It's probably not DICOM")
                    print("Trying to move NON-DICOM file to:" + self.error_directory)
                    #Handle Error and send notifiction
                    #Move to error directory
                    #shutil.move(file, self.error_directory)
                    try:
                        shutil.move(file, self.error_directory)
                    except shutil.Error as e:
                        print('Error: %s' % e)
                        os.remove(file)
                        pass
                        # eg. source or destination doesn't exist
                    except IOError as e:
                        print('Error: %s' % e.strerror)
                        os.remove(file)
                        pass


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

