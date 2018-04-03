import threading
import time
import queue

import pydicom
import json
from lxml import etree

import ibm_db
print (ibm_db.__version__)
#import DB2ConnectDB





'''
Global variable used by threads and locking
'''
# Hold list of db's to process
global_db_list = []
# Hold data collected from list of db's to process
global_fifo_q = queue.Queue()
#Thread lock
tLock = threading.Lock()


def get_db_list (admin_db, user):
    global global_db_list
    global global_fifo_q
    db_list = []
    tLock.acquire()
    print ("db_list has acquired a lock")

    dbconn = ibm_db.connect('dashdb', 'vrz67438', '12r1p9wmb-z2s4t5')
    print (dbconn)

    tLock.release()
    print ("db_list has completed lock released")
    db_list = ['Testing1','Testing2','Testing3'  ]
    global_fifo_q.put(db_list)
    #global_db_list = db_list



class DB2ConnectDB:
    def __init__(self, target_db, db_user, db_password):
        self.target_db = target_db
        self.db_user = db_user
        self.db_password = db_password
        self.dbconn = ibm_db.connect(self.target_db, self.db_user, self.db_password)

class StoreData(DB2ConnectDB):
    pass
    def retrieve_and_store(self):
        global global_fifo_q
        print ('In Store Data thread')
        print (self.dbconn)
        while True:
            if not global_fifo_q.empty():
                data_from_q = global_fifo_q.get()
                print (data_from_q)

def Main():

    db_name = 'dashdb'
    db_user = 'vrz67438'
    db_password = '12r1p9wmb-z2s4t5'


    t1 = threading.Thread(target=get_db_list, name = "Get db list", args=("ADM1P","tu01945",))
    t1.start()
    t1.join()

    s = StoreData(db_name, db_user, db_password)
    st = threading.Thread(target=s.retrieve_and_store, name = 'Retrieve Q Data', args=())
    st.start()
    st.join()



if __name__ == '__main__':
    Main()
