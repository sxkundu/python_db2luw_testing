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
                    manufacturerModelName = dcm_file.ManufacturerModelName
                    entranceDose = dcm_file.EntranceDoseInmGy
                    studyInstanceUID = dcm_file.StudyInstanceUID
                    seriesInstanceUID = dcm_file.SeriesInstanceUID
                    parsed_result["testInfo"] = {
                        "testType": testType,
                        "studyDate": studyDate,
                        "studyTime": studyTime,
                        "deviceSerialNumber": deviceSerialNumber,
                        "institutuionName": institutionName,
                        "manufacturerModelName": manufacturerModelName,
                        "manufacturer": manufacturer,
                        "entranceDoseinmGy": entranceDose,
                        "studyInstanceUID": studyInstanceUID,
                        "seriesInstanceUID": seriesInstanceUID
                    }
                    user_firstName = dcm_file.PatientName.given_name
                    user_lastName = dcm_file.PatientName.family_name
                    user_id = dcm_file.PatientID
                    ethnic_group = dcm_file.EthnicGroup
                    user_birthdate = dcm_file.PatientBirthDate
                    user_sex = dcm_file.PatientSex
                    user_age = dcm_file.PatientAge
                    user_Size = dcm_file.PatientSize
                    user_Weight = dcm_file.PatientWeight
                    parsed_result["userInfo"] = {
                        "firstName": user_firstName,
                        "lastName": user_lastName,
                        "email": user_id,
                        "ethnicGroup": ethnic_group,
                        "birthDate": user_birthdate,
                        "userSex": user_sex,
                        "userAge": user_age,
                        "userSize": user_Size,
                        "userWeight": user_Weight
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

                    #print ("XML_ROOT", xml_root)

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



class ReadDICOMFile:
    def __init__(self, input_dicom_file, error_directory):
        self.input_dicom_file = input_dicom_file
        self.error_directory = error_directory
        self.parsed_result = None
        self.json_result = None
        #self.BMD_result =[]
        print ('In Single ReadDICOMFile...')

    def get_parsed_result(self):
        return self.parsed_result

    def get_parsed_result_bmd(self):

        return (self.parsed_result["testInfo"]["seriesInstanceUID"],
                self.parsed_result["BMD"]["arms"]["bmd"],
                self.parsed_result["BMD"]["head"]["bmd"],
                self.parsed_result["BMD"]["legs"]["bmd"],
                self.parsed_result["BMD"]["pelvis"]["bmd"],
                self.parsed_result["BMD"]["ribs"]["bmd"],
                self.parsed_result["BMD"]["spine"]["bmd"],
                self.parsed_result["BMD"]["trunk"]["bmd"],
                self.parsed_result["BMD"]["total"]["bmd"],
                self.parsed_result["BMD"]["total"]["bmdTscore"],
                self.parsed_result["BMD"]["total"]["bmdZscore"]
                )


    def get_parsed_result_bodycomposition(self):

        return (self.parsed_result["testInfo"]["seriesInstanceUID"],
                self.parsed_result["bodyComposition"]["android"]["bmc"],
                self.parsed_result["bodyComposition"]["android"]["fatMass"],
                self.parsed_result["bodyComposition"]["android"]["leanMass"],
                self.parsed_result["bodyComposition"]["android"]["regionPfat"],
                self.parsed_result["bodyComposition"]["android"]["totalMass"],

                self.parsed_result["bodyComposition"]["armLeft"]["bmc"],
                self.parsed_result["bodyComposition"]["armLeft"]["fatMass"],
                self.parsed_result["bodyComposition"]["armLeft"]["leanMass"],
                self.parsed_result["bodyComposition"]["armLeft"]["regionPfat"],
                self.parsed_result["bodyComposition"]["armLeft"]["totalMass"],

                self.parsed_result["bodyComposition"]["armRight"]["bmc"],
                self.parsed_result["bodyComposition"]["armRight"]["fatMass"],
                self.parsed_result["bodyComposition"]["armRight"]["leanMass"],
                self.parsed_result["bodyComposition"]["armRight"]["regionPfat"],
                self.parsed_result["bodyComposition"]["armRight"]["totalMass"],

                self.parsed_result["bodyComposition"]["arms"]["bmc"],
                self.parsed_result["bodyComposition"]["arms"]["fatMass"],
                self.parsed_result["bodyComposition"]["arms"]["leanMass"],
                self.parsed_result["bodyComposition"]["arms"]["regionPfat"],
                self.parsed_result["bodyComposition"]["arms"]["totalMass"],

                self.parsed_result["bodyComposition"]["armsDiff"]["bmc"],
                self.parsed_result["bodyComposition"]["armsDiff"]["fatMass"],
                self.parsed_result["bodyComposition"]["armsDiff"]["leanMass"],
                self.parsed_result["bodyComposition"]["armsDiff"]["regionPfat"],
                self.parsed_result["bodyComposition"]["armsDiff"]["totalMass"],

                self.parsed_result["bodyComposition"]["gynoid"]["bmc"],
                self.parsed_result["bodyComposition"]["gynoid"]["fatMass"],
                self.parsed_result["bodyComposition"]["gynoid"]["leanMass"],
                self.parsed_result["bodyComposition"]["gynoid"]["regionPfat"],
                self.parsed_result["bodyComposition"]["gynoid"]["totalMass"],

                self.parsed_result["bodyComposition"]["legLeft"]["bmc"],
                self.parsed_result["bodyComposition"]["legLeft"]["fatMass"],
                self.parsed_result["bodyComposition"]["legLeft"]["leanMass"],
                self.parsed_result["bodyComposition"]["legLeft"]["regionPfat"],
                self.parsed_result["bodyComposition"]["legLeft"]["totalMass"],

                self.parsed_result["bodyComposition"]["legRight"]["bmc"],
                self.parsed_result["bodyComposition"]["legRight"]["fatMass"],
                self.parsed_result["bodyComposition"]["legRight"]["leanMass"],
                self.parsed_result["bodyComposition"]["legRight"]["regionPfat"],
                self.parsed_result["bodyComposition"]["legRight"]["totalMass"],

                self.parsed_result["bodyComposition"]["legs"]["bmc"],
                self.parsed_result["bodyComposition"]["legs"]["fatMass"],
                self.parsed_result["bodyComposition"]["legs"]["leanMass"],
                self.parsed_result["bodyComposition"]["legs"]["regionPfat"],
                self.parsed_result["bodyComposition"]["legs"]["totalMass"],

                self.parsed_result["bodyComposition"]["legsDiff"]["bmc"],
                self.parsed_result["bodyComposition"]["legsDiff"]["fatMass"],
                self.parsed_result["bodyComposition"]["legsDiff"]["leanMass"],
                self.parsed_result["bodyComposition"]["legsDiff"]["regionPfat"],
                self.parsed_result["bodyComposition"]["legsDiff"]["totalMass"],

                self.parsed_result["bodyComposition"]["totalLeft"]["bmc"],
                self.parsed_result["bodyComposition"]["totalLeft"]["fatMass"],
                self.parsed_result["bodyComposition"]["totalLeft"]["leanMass"],
                self.parsed_result["bodyComposition"]["totalLeft"]["regionPfat"],
                self.parsed_result["bodyComposition"]["totalLeft"]["totalMass"],

                self.parsed_result["bodyComposition"]["totalRight"]["bmc"],
                self.parsed_result["bodyComposition"]["totalRight"]["fatMass"],
                self.parsed_result["bodyComposition"]["totalRight"]["leanMass"],
                self.parsed_result["bodyComposition"]["totalRight"]["regionPfat"],
                self.parsed_result["bodyComposition"]["totalRight"]["totalMass"],

                self.parsed_result["bodyComposition"]["total"]["bmc"],
                self.parsed_result["bodyComposition"]["total"]["fatMass"],
                self.parsed_result["bodyComposition"]["total"]["leanMass"],
                self.parsed_result["bodyComposition"]["total"]["regionPfat"],
                self.parsed_result["bodyComposition"]["total"]["totalMass"],

                self.parsed_result["bodyComposition"]["totalDiff"]["bmc"],
                self.parsed_result["bodyComposition"]["totalDiff"]["fatMass"],
                self.parsed_result["bodyComposition"]["totalDiff"]["leanMass"],
                self.parsed_result["bodyComposition"]["totalDiff"]["regionPfat"],
                self.parsed_result["bodyComposition"]["totalDiff"]["totalMass"],

                self.parsed_result["bodyComposition"]["trunkLeft"]["bmc"],
                self.parsed_result["bodyComposition"]["trunkLeft"]["fatMass"],
                self.parsed_result["bodyComposition"]["trunkLeft"]["leanMass"],
                self.parsed_result["bodyComposition"]["trunkLeft"]["regionPfat"],
                self.parsed_result["bodyComposition"]["trunkLeft"]["totalMass"],

                self.parsed_result["bodyComposition"]["trunkRight"]["bmc"],
                self.parsed_result["bodyComposition"]["trunkRight"]["fatMass"],
                self.parsed_result["bodyComposition"]["trunkRight"]["leanMass"],
                self.parsed_result["bodyComposition"]["trunkRight"]["regionPfat"],
                self.parsed_result["bodyComposition"]["trunkRight"]["totalMass"],

                self.parsed_result["bodyComposition"]["trunk"]["bmc"],
                self.parsed_result["bodyComposition"]["trunk"]["fatMass"],
                self.parsed_result["bodyComposition"]["trunk"]["leanMass"],
                self.parsed_result["bodyComposition"]["trunk"]["regionPfat"],
                self.parsed_result["bodyComposition"]["trunk"]["totalMass"],

                self.parsed_result["bodyComposition"]["trunkDiff"]["bmc"],
                self.parsed_result["bodyComposition"]["trunkDiff"]["fatMass"],
                self.parsed_result["bodyComposition"]["trunkDiff"]["leanMass"],
                self.parsed_result["bodyComposition"]["trunkDiff"]["regionPfat"],
                self.parsed_result["bodyComposition"]["trunkDiff"]["totalMass"]
                )

    def get_parsed_result_UID(self):
        return self.parsed_result["testInfo"]["seriesInstanceUID"]


    def parse(self):
        global global_fifo_q
        #os.chdir(self.input_dicom_file)
        #while True:
        #    time.sleep(10)
        #for file in glob.glob("*.dcm"):
        file = self.input_dicom_file
        #print(file)
        if is_dicom(file):
            print("It is indeed DICOM!")
            dcm_file = pydicom.dcmread(file)
            #print(dcm_file)

            parsed_result = {}
            testType = dcm_file.StudyDescription
            studyDate = dcm_file.StudyDate
            studyTime = dcm_file.StudyTime
            deviceSerialNumber = dcm_file.DeviceSerialNumber
            institutionName = dcm_file.InstitutionName
            manufacturer = dcm_file.Manufacturer
            manufacturerModelName = dcm_file.ManufacturerModelName
            entranceDose = dcm_file.EntranceDoseInmGy
            studyInstanceUID = dcm_file.StudyInstanceUID
            seriesInstanceUID = dcm_file.SeriesInstanceUID
            parsed_result["testInfo"] = {
                "testType": testType,
                "studyDate": studyDate,
                "studyTime": studyTime,
                "deviceSerialNumber": deviceSerialNumber,
                "institutuionName": institutionName,
                "manufacturerModelName": manufacturerModelName,
                "manufacturer": manufacturer,
                "entranceDoseinmGy": entranceDose,
                "studyInstanceUID": studyInstanceUID,
                "seriesInstanceUID": seriesInstanceUID
            }
            user_firstName = dcm_file.PatientName.given_name
            user_lastName = dcm_file.PatientName.family_name
            ethnic_group = dcm_file.EthnicGroup
            user_birthdate = dcm_file.PatientBirthDate
            user_sex = dcm_file.PatientSex
            user_id = dcm_file.PatientID
            if user_id is '':
                user_id = str(user_firstName) + "." + str(user_lastName) + "." + str(user_birthdate) + "@noemail.unk"
            user_age = dcm_file.PatientAge
            user_Size = dcm_file.PatientSize
            user_Weight = dcm_file.PatientWeight
            parsed_result["userInfo"] = {
                "firstName": user_firstName,
                "lastName": user_lastName,
                "email": user_id,
                "ethnicGroup": ethnic_group,
                "birthDate": user_birthdate,
                "userSex": user_sex,
                "userAge": user_age,
                "userSize": user_Size,
                "userWeight": user_Weight
            }

            xml_string = dcm_file.ImageComments
            xml_root = etree.fromstring(xml_string)

            #print('XML_root',xml_root.iter('ROI') )

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
                #print (regionName)
                parsed_result["BMD"][regionName] = {}
                for reading in leaf.iter():
                    #print (reading.text,'***', reading.tag)
                    if reading.text is None:
                        continue
                    elif reading.text is '-':
                        continue
                    key = lowerCamelCase(reading.tag)
                    #units = reading.attrib['units'].strip()
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
            self.json_result = json.dumps(parsed_result)
            print(self.json_result)

            self.parsed_result = parsed_result

            # Add data to queue
            #data_to_process = (user_id, self.json_result, studyDate, studyTime, file)
            '''
            data_to_process = (user_id, self.json_result, studyDate, studyTime, file,
                               user_firstName, user_lastName, user_birthdate, studyInstanceUID,
                               (self.parsed_result["testInfo"]["seriesInstanceUID"],
                                self.parsed_result["BMD"]["arms"]["bmd"],
                                self.parsed_result["BMD"]["head"]["bmd"],
                                self.parsed_result["BMD"]["legs"]["bmd"],
                                self.parsed_result["BMD"]["pelvis"]["bmd"],
                                self.parsed_result["BMD"]["ribs"]["bmd"],
                                self.parsed_result["BMD"]["spine"]["bmd"],
                                self.parsed_result["BMD"]["trunk"]["bmd"],
                                self.parsed_result["BMD"]["total"]["bmd"],
                                self.parsed_result["BMD"]["total"]["bmdTscore"],
                                self.parsed_result["BMD"]["total"]["bmdZscore"]
                                )

                               )
            '''
            data_to_process = (user_id, self.json_result, studyDate, studyTime, file,
                               user_firstName, user_lastName, user_birthdate, studyInstanceUID
                               )
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




class StoreDataSinglePass(PGConnectDB):
    pass

    def retrieve_and_store(self, incomplete_directory, processed_directory):
        self.incomplete_directory = incomplete_directory
        self.processed_directory = processed_directory
        global global_fifo_q
        print('In Store Data Single PAss')
        print(self.conn)
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
            DXA_email_UID = (data_from_q[0],)
            self.cursor.execute(sqlselect, DXA_email_UID)
            print("successfully selected records")
            self.uuid = self.cursor.fetchone()
            print(self.uuid)

            if not (self.uuid):
                print("Missing user info, will insert with generated DXA email id")

                self.firstname = str(data_from_q[5])
                self.lastname = str(data_from_q[6])
                self.birthdate = str(data_from_q[7])
                self.email = str(data_from_q[0])
                self.dexafitpatientid = str(data_from_q[0])
                print (self.firstname, self.lastname, self.email, self.dexafitpatientid)


                self.sqlinsert_userinfo = "INSERT into dexafit.userinfo (firstname, lastname, email, dexafitpatientid, birthday) VALUES(%s, %s, %s, %s, %s);"
                DXA_user_details = (self.firstname ,self.lastname ,self.email, self.dexafitpatientid, self.birthdate, )
                #print (self.sqlinsert_userinfo)
                self.cursor.execute(self.sqlinsert_userinfo, DXA_user_details)
                # Get generated UUID
                sqlselect = "select userid from dexafit.userinfo where dexafitpatientid = %s;"
                DXA_email_UID = (data_from_q[0],)
                self.cursor.execute(sqlselect, DXA_email_UID)
                self.uuid = self.cursor.fetchone()

            if (self.uuid):
                print("UUID found")
                # insert with uuid and json
                self.json_result = data_from_q[1]
                self.studyDate = data_from_q[2]
                self.studyTime = data_from_q[3]
                self.studyInstanceUID = data_from_q[8]

                # Error handing needed for SQL and connection.
                # Insert into json table
                self.sqlinsert = "INSERT into dxa.dxatest_json (userid, testdate, testtime, results, studyinstanceuid) VALUES(%s, %s, %s, %s, %s);"
                self.dexa = (self.uuid, self.studyDate, self.studyTime, self.json_result, self.studyInstanceUID)

                try:
                    self.cursor.execute(self.sqlinsert, self.dexa)
                    print("Successfully inserted records dxa.dxatest_json")
                except psycopg2.DatabaseError as error:
                    return(error)


                #insert in BMD table
                #self.BMD_tuple = data_from_q[9] + self.uuid
                #print (self.BMD_tuple)

                self.parsed_result = json.loads(self.json_result)
                #print(self.parse_result)


                self.sqlinsert_bmd = "INSERT into dxa.dxatest_bmd (userid, studyinstanceuid, arms, head, legs, pelvis" \
                                     ", ribs, spine, trunk, total_bmd, total_bmdtscore, total_bmdzscore ) " \
                                     "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s );"
                self.dexa_bmd = (self.uuid,
                       self.parsed_result["testInfo"]["seriesInstanceUID"],
                       self.parsed_result["BMD"]["arms"]["bmd"],
                       self.parsed_result["BMD"]["head"]["bmd"],
                       self.parsed_result["BMD"]["legs"]["bmd"],
                       self.parsed_result["BMD"]["pelvis"]["bmd"],
                       self.parsed_result["BMD"]["ribs"]["bmd"],
                       self.parsed_result["BMD"]["spine"]["bmd"],
                       self.parsed_result["BMD"]["trunk"]["bmd"],
                       self.parsed_result["BMD"]["total"]["bmd"],
                       self.parsed_result["BMD"]["total"]["bmdTscore"],
                       self.parsed_result["BMD"]["total"]["bmdZscore"]
                       )
                try:
                    self.cursor.execute(self.sqlinsert_bmd, self.dexa_bmd)
                    print("BMD Successfully inserted records dxa.dxatest_bmd")
                except psycopg2.DatabaseError as error:
                    return(error)

                self.sqlinsert_bodycomposition = "INSERT into dxa.dxatest_bodycomposition (userid, studyinstanceuid" \
                                                 ",android_bmc, android_fatmass, android_leanmass, android_regionpfat, android_totalmass" \
                                                 ",armleft_bmc ,armleft_fatmass ,armleft_leanmass,armleft_regionpfat,armleft_totalmass" \
                                                 ",armright_bmc,armright_fatmass ,armright_leanmass ,armright_regionpfat,armright_totalmass" \
                                                 ",arms_bmc,arms_fatmass,arms_leanmass,arms_regionpfat,arms_totalmass" \
                                                 ",armsdiff_bmc,armsdiff_fatmass,armsdiff_leanmass,armsdiff_regionpfat,armsdiff_totalmass" \
                                                 ",gynoid_bmc,gynoid_fatmass,gynoid_leanmass,gynoid_regionpfat,gynoid_totalmass" \
                                                 ",legleft_bmc,legleft_fatmass,legleft_leanmass,legleft_regionpfat,legleft_totalmass"\
                                                 ",legright_bmc,legright_fatmass,legright_leanmass,legright_regionpfat,legright_totalmass"\
                                                 ",legs_bmc,legs_fatmass,legs_leanmass,legs_regionpfat,legs_totalmass" \
                                                 ",legsdiff_bmc,legsdiff_fatmass,legsdiff_leanmass,legsdiff_regionpfat,legsdiff_totalmass"\
                                                 ",total_bmc,total_fatmass,total_leanmass,total_regionpfat,total_totalmass,totaldiff_bmc"\
                                                 ",totaldiff_fatmass,totaldiff_leanmass,totaldiff_regionpfat,totaldiff_totalmass"\
                                                 ",totalleft_bmc,totalleft_fatmass,totalleft_leanmass,totalleft_regionpfat,totalleft_totalmass"\
                                                 ",totalright_bmc,totalright_fatmass,totalright_leanmass,totalright_regionpfat,totalright_totalmass"\
                                                 ",trunk_bmc,trunk_fatmass,trunk_leanmass,trunk_regionpfat,trunk_totalmass"\
                                                 ",trunkdiff_bmc,trunkdiff_fatmass,trunkdiff_leanmass,trunkdiff_regionpfat,trunkdiff_totalmass"\
                                                 ",trunkleft_bmc,trunkleft_fatmass,trunkleft_leanmass,trunkleft_regionpfat,trunkleft_totalmass"\
                                                 ",trunkright_bmc,trunkright_fatmass,trunkright_leanmass,trunkright_regionpfat,trunkright_totalmass"\
                                                 ",vatmass ) " \
                                                 "VALUES(%s, %s, " \
                                                 "%s, %s, %s, %s, %s, " \
                                                 "%s, %s, %s, %s, %s, " \
                                                 "%s, %s, %s, %s, %s, " \
                                                 "%s, %s, %s, %s, %s, " \
                                                 "%s, %s, %s, %s, %s, " \
                                                 "%s, %s, %s, %s, %s, " \
                                                 "%s, %s, %s, %s, %s, " \
                                                 "%s, %s, %s, %s, %s, " \
                                                 "%s, %s, %s, %s, %s, " \
                                                 "%s, %s, %s, %s, %s, " \
                                                 "%s, %s, %s, %s, %s, " \
                                                 "%s, %s, %s, %s, %s, " \
                                                 "%s, %s, %s, %s, %s, " \
                                                 "%s, %s, %s, %s, %s, " \
                                                 "%s, %s, %s, %s, %s, " \
                                                 "%s, %s, %s, %s, %s, " \
                                                 "%s, %s, %s, %s, %s, " \
                                                 "%s, %s, %s, %s, %s, " \
                                                 "%s ); "


                self.dexa_bodycomposition =        (self.uuid,
                         self.parsed_result["testInfo"]["seriesInstanceUID"],
                         self.parsed_result["bodyComposition"]["android"]["bmc"],
                         self.parsed_result["bodyComposition"]["android"]["fatMass"],
                         self.parsed_result["bodyComposition"]["android"]["leanMass"],
                         self.parsed_result["bodyComposition"]["android"]["regionPfat"],
                         self.parsed_result["bodyComposition"]["android"]["totalMass"],

                         self.parsed_result["bodyComposition"]["armLeft"]["bmc"],
                         self.parsed_result["bodyComposition"]["armLeft"]["fatMass"],
                         self.parsed_result["bodyComposition"]["armLeft"]["leanMass"],
                         self.parsed_result["bodyComposition"]["armLeft"]["regionPfat"],
                         self.parsed_result["bodyComposition"]["armLeft"]["totalMass"],

                         self.parsed_result["bodyComposition"]["armRight"]["bmc"],
                         self.parsed_result["bodyComposition"]["armRight"]["fatMass"],
                         self.parsed_result["bodyComposition"]["armRight"]["leanMass"],
                         self.parsed_result["bodyComposition"]["armRight"]["regionPfat"],
                         self.parsed_result["bodyComposition"]["armRight"]["totalMass"],

                         self.parsed_result["bodyComposition"]["arms"]["bmc"],
                         self.parsed_result["bodyComposition"]["arms"]["fatMass"],
                         self.parsed_result["bodyComposition"]["arms"]["leanMass"],
                         self.parsed_result["bodyComposition"]["arms"]["regionPfat"],
                         self.parsed_result["bodyComposition"]["arms"]["totalMass"],

                         self.parsed_result["bodyComposition"]["armsDiff"]["bmc"],
                         self.parsed_result["bodyComposition"]["armsDiff"]["fatMass"],
                         self.parsed_result["bodyComposition"]["armsDiff"]["leanMass"],
                         self.parsed_result["bodyComposition"]["armsDiff"]["regionPfat"],
                         self.parsed_result["bodyComposition"]["armsDiff"]["totalMass"],

                         self.parsed_result["bodyComposition"]["gynoid"]["bmc"],
                         self.parsed_result["bodyComposition"]["gynoid"]["fatMass"],
                         self.parsed_result["bodyComposition"]["gynoid"]["leanMass"],
                         self.parsed_result["bodyComposition"]["gynoid"]["regionPfat"],
                         self.parsed_result["bodyComposition"]["gynoid"]["totalMass"],

                         self.parsed_result["bodyComposition"]["legLeft"]["bmc"],
                         self.parsed_result["bodyComposition"]["legLeft"]["fatMass"],
                         self.parsed_result["bodyComposition"]["legLeft"]["leanMass"],
                         self.parsed_result["bodyComposition"]["legLeft"]["regionPfat"],
                         self.parsed_result["bodyComposition"]["legLeft"]["totalMass"],

                         self.parsed_result["bodyComposition"]["legRight"]["bmc"],
                         self.parsed_result["bodyComposition"]["legRight"]["fatMass"],
                         self.parsed_result["bodyComposition"]["legRight"]["leanMass"],
                         self.parsed_result["bodyComposition"]["legRight"]["regionPfat"],
                         self.parsed_result["bodyComposition"]["legRight"]["totalMass"],

                         self.parsed_result["bodyComposition"]["legs"]["bmc"],
                         self.parsed_result["bodyComposition"]["legs"]["fatMass"],
                         self.parsed_result["bodyComposition"]["legs"]["leanMass"],
                         self.parsed_result["bodyComposition"]["legs"]["regionPfat"],
                         self.parsed_result["bodyComposition"]["legs"]["totalMass"],

                         self.parsed_result["bodyComposition"]["legsDiff"]["bmc"],
                         self.parsed_result["bodyComposition"]["legsDiff"]["fatMass"],
                         self.parsed_result["bodyComposition"]["legsDiff"]["leanMass"],
                         self.parsed_result["bodyComposition"]["legsDiff"]["regionPfat"],
                         self.parsed_result["bodyComposition"]["legsDiff"]["totalMass"],

                         self.parsed_result["bodyComposition"]["totalLeft"]["bmc"],
                         self.parsed_result["bodyComposition"]["totalLeft"]["fatMass"],
                         self.parsed_result["bodyComposition"]["totalLeft"]["leanMass"],
                         self.parsed_result["bodyComposition"]["totalLeft"]["regionPfat"],
                         self.parsed_result["bodyComposition"]["totalLeft"]["totalMass"],

                         self.parsed_result["bodyComposition"]["totalRight"]["bmc"],
                         self.parsed_result["bodyComposition"]["totalRight"]["fatMass"],
                         self.parsed_result["bodyComposition"]["totalRight"]["leanMass"],
                         self.parsed_result["bodyComposition"]["totalRight"]["regionPfat"],
                         self.parsed_result["bodyComposition"]["totalRight"]["totalMass"],

                         self.parsed_result["bodyComposition"]["total"]["bmc"],
                         self.parsed_result["bodyComposition"]["total"]["fatMass"],
                         self.parsed_result["bodyComposition"]["total"]["leanMass"],
                         self.parsed_result["bodyComposition"]["total"]["regionPfat"],
                         self.parsed_result["bodyComposition"]["total"]["totalMass"],

                         self.parsed_result["bodyComposition"]["totalDiff"]["bmc"],
                         self.parsed_result["bodyComposition"]["totalDiff"]["fatMass"],
                         self.parsed_result["bodyComposition"]["totalDiff"]["leanMass"],
                         self.parsed_result["bodyComposition"]["totalDiff"]["regionPfat"],
                         self.parsed_result["bodyComposition"]["totalDiff"]["totalMass"],

                         self.parsed_result["bodyComposition"]["trunkLeft"]["bmc"],
                         self.parsed_result["bodyComposition"]["trunkLeft"]["fatMass"],
                         self.parsed_result["bodyComposition"]["trunkLeft"]["leanMass"],
                         self.parsed_result["bodyComposition"]["trunkLeft"]["regionPfat"],
                         self.parsed_result["bodyComposition"]["trunkLeft"]["totalMass"],

                         self.parsed_result["bodyComposition"]["trunkRight"]["bmc"],
                         self.parsed_result["bodyComposition"]["trunkRight"]["fatMass"],
                         self.parsed_result["bodyComposition"]["trunkRight"]["leanMass"],
                         self.parsed_result["bodyComposition"]["trunkRight"]["regionPfat"],
                         self.parsed_result["bodyComposition"]["trunkRight"]["totalMass"],

                         self.parsed_result["bodyComposition"]["trunk"]["bmc"],
                         self.parsed_result["bodyComposition"]["trunk"]["fatMass"],
                         self.parsed_result["bodyComposition"]["trunk"]["leanMass"],
                         self.parsed_result["bodyComposition"]["trunk"]["regionPfat"],
                         self.parsed_result["bodyComposition"]["trunk"]["totalMass"],

                         self.parsed_result["bodyComposition"]["trunkDiff"]["bmc"],
                         self.parsed_result["bodyComposition"]["trunkDiff"]["fatMass"],
                         self.parsed_result["bodyComposition"]["trunkDiff"]["leanMass"],
                         self.parsed_result["bodyComposition"]["trunkDiff"]["regionPfat"],
                         self.parsed_result["bodyComposition"]["trunkDiff"]["totalMass"],

                         self.parsed_result["visceralFat"]["estimatedVisceralAdiposeTissue"]["vatMass"]
                         )

                try:
                    self.cursor.execute(self.sqlinsert_bodycomposition, self.dexa_bodycomposition)
                    print("BMD Successfully inserted records dxa.dxatest_bodycomposition")
                except psycopg2.DatabaseError as error:
                    return(error)



                # Remove after inserts work
                try:
                    self.conn.commit()
                    return("Success")
                except psycopg2.DatabaseError as error:
                    return(error)


                '''
                #Un comment after extracting from JSON
                #Move Dicom File
                print ("Trying to move DICOM file to:"+self.processed_directory)
                source = self.file
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
                '''
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
