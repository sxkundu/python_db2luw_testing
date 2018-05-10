import threading
import sys, getopt

import pprint

#from Dexascan_Parse_and_Store_in_PG_v4 import *
from Dexascan_Parse_and_Store_in_PG_v4 import StoreData
from Dexascan_Parse_and_Store_in_PG_v4 import ReadDICOMFiles

from Dexascan_Parse_and_Store_in_PG_v4 import ReadDICOMFile
from Dexascan_Parse_and_Store_in_PG_v4 import StoreDataSinglePass


def Main(argv):
    inputfile = None
    try:
        opts, args = getopt.getopt(argv, "hi:", ["ifile="])
    except getopt.GetoptError:
        print ('Parse_and_Store_PG.py -i <inputfile>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('Parse_and_Store_PG.py -i <inputfile>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
    print('Input file is "', inputfile)

    '''
    JSON Parse Execution
    '''
    # DICOM File has been parsed to JSON and saved to the pending directory.
    #pending_directory = 'c:/temp/pending_dexafit_files/'
    error_directory = 'c:/temp/error_dexafit_files/'
    #f = ReadDICOMFiles(pending_directory, error_directory)
    #ft = threading.Thread(target=f.retrieve, name='Retrieve DICOM Data', args=())
    #ft.start()
    f = ReadDICOMFile(inputfile, error_directory)
    f.parse()
    parsed_dictionary_data = f.get_parsed_result()
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(parsed_dictionary_data)
    #print(parsed_dictionary_data.items())

    parsed_dictionary_data_bmd = f.get_parsed_result_bmd()
    #print(parsed_dictionary_data_BMD.items())
    pp.pprint(parsed_dictionary_data_bmd)

    parsed_dictionary_data_UID = f.get_parsed_result_UID()
    # print(parsed_dictionary_data_BMD.items())
    pp.pprint(parsed_dictionary_data_UID)

    parsed_dictionary_data_bodycomp = f.get_parsed_result_bodycomposition()
    # print(parsed_dictionary_data_BMD.items())
    pp.pprint(parsed_dictionary_data_bodycomp)

    # Read DB  info from text file

    with open('PG_Connection.cfg', 'r') as pg_db_cfg:
        dsn_database = pg_db_cfg.readline().strip()
        dsn_hostname = pg_db_cfg.readline().strip()
        dsn_port = pg_db_cfg.readline().strip()
        dsn_uid = pg_db_cfg.readline().strip()
        dsn_pwd = pg_db_cfg.readline().strip()


    incomplete_directory = 'c:/temp/incomplete_dexafit_files'
    processed_directory = 'c:/temp/processed_dexafit_files/'


    #Code for threading
    '''
    s = StoreData(dsn_hostname, dsn_port, dsn_database, dsn_uid, dsn_pwd)
    st = threading.Thread(target=s.retrieve_and_store, name = 'Retrieve Q Data and insert into PG', args=(incomplete_directory,processed_directory,))
    st.start()
    '''
    '''
    s = StoreDataSinglePass(dsn_hostname, dsn_port, dsn_database, dsn_uid, dsn_pwd)
    st = threading.Thread(target=s.retrieve_and_store, name='Retrieve Q Data and insert into PG',
                          args=(incomplete_directory, processed_directory,))
    st.start()
    st.join()
    '''
if __name__ == '__main__':
    Main(sys.argv[1:])


