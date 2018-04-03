import pydicom
from pydicom.misc import is_dicom
import os
#import dicom
import json
from lxml import etree
# import psycopg2
# import psycopg2.extras
from boxsdk import Client, OAuth2
from boxsdk import JWTAuth
from io import BytesIO

#pip install "boxsdk[jwt]" ###(View-> Terminal)

'''
Helper Methods
'''


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


auth = JWTAuth(
    client_id='dc100pf8a6yorgujddbdi56twd1a6r4k',
    client_secret='XMIooxaNZwXFevSUbU94luaeQ9VwkPVv',
    enterprise_id='37228416',
    jwt_key_id='ir6fahr5',
    rsa_private_key_file_sys_path='c:/temp/BOXPrivatekey/private.pem'

)

client = Client(auth)

dexafituser = client.user('3164793504')

auth.authenticate_app_user(dexafituser)
'''
file_id='270819570112'
url = client.file(file_id).content()

#url = client.file(file_id='270819570112').content()

#print(url)

with BytesIO(url) as dcmbox:
    dcm_file = pydicom.dcmread(dcmbox)
    #print (dcm_file)
    file_destination="c:/temp/"+file_id+".dcm"
    #pydicom.dcmwrite("c:/temp/altered_file.dcm", dcm_file)
    pydicom.dcmwrite(file_destination, dcm_file)

#274974226735, 273962712480, 273961696958, 273961309534, 273939676673
'''

#file_id_list=['274974226735', '273962712480', '273961696958', '273961309534', '273939676673']
file_id_list=['270819570112']
for file_id in file_id_list:
    print (file_id)
    url = client.file(file_id).content()
    with BytesIO(url) as dcmbox:
        dcm_file = pydicom.dcmread(dcmbox)
        file_destination="c:/temp/pending_dexafit_files/DICOM"+file_id+".dcm"
        pydicom.dcmwrite(file_destination, dcm_file)


'''
# Import two classes from the boxsdk module - Client and OAuth2
from boxsdk import Client, OAuth2

# Define client ID, client secret, and developer token.
CLIENT_ID = None
CLIENT_SECRET = None
ACCESS_TOKEN = None

# Read app info from text file
with open('app.cfg', 'r') as app_cfg:
    CLIENT_ID = app_cfg.readline()
    CLIENT_SECRET = app_cfg.readline()
    ACCESS_TOKEN = app_cfg.readline()


http://opensource.box.com/box-python-sdk/tutorials/intro.html
'''

