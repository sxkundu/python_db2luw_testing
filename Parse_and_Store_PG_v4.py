import threading
#from Dexascan_Parse_and_Store_in_PG_v1 import *
from Dexascan_Parse_and_Store_in_PG_v1 import StoreData
from Dexascan_Parse_and_Store_in_PG_v1 import ReadDICOMFiles


def Main():
    '''
    JSON Parse Execution
    '''
    # DICOM File has been parsed to JSON and saved to the pending directory.
    pending_directory = 'c:/temp/pending_dexafit_files/'
    error_directory = 'c:/temp/error_dexafit_files/'
    f = ReadDICOMFiles(pending_directory, error_directory)
    ft = threading.Thread(target=f.retrieve, name='Retrieve DICOM Data', args=())
    ft.start()


    # Read DB  info from text file
    with open('PG_Connection.cfg', 'r') as pg_db_cfg:
        dsn_database = pg_db_cfg.readline().strip()
        dsn_hostname = pg_db_cfg.readline().strip()
        dsn_port = pg_db_cfg.readline().strip()
        dsn_uid = pg_db_cfg.readline().strip()
        dsn_pwd = pg_db_cfg.readline().strip()

    s = StoreData(dsn_hostname ,dsn_port,  dsn_database, dsn_uid, dsn_pwd)

    incomplete_directory = 'c:/temp/incomplete_dexafit_files'
    processed_directory = 'c:/temp/processed_dexafit_files/'
    st = threading.Thread(target=s.retrieve_and_store, name = 'Retrieve Q Data and insert into PG', args=(incomplete_directory,processed_directory,))
    st.start()

if __name__ == '__main__':
    Main()


