
import threading
import time
import queue

'''
Sample framework to collect data from multiple sources and insert into repos.
Add args for sample interval
Sudip
'''

import ibm_db
print (ibm_db.__version__)


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
    db_list = []
    tLock.acquire()
    print ("db_list has acquired a lock")
    # Get password
    str = open('C:/Users/sxk11/PycharmProjects/Safari_1/test.txt', 'r').read()
    conn = ibm_db.connect('ADM1P', 'tu01945', str)
    sql = 'SELECT * FROM DB2ADM1P.UDB_DB_NAMES ' \
          'where ' \
          'db_all_chk_ind = \'Y\' ' \
          'and db_alias in (\'LSS1D\', \'EDT5D\', \'EDT6D\', \'DBI1D\') '
          # 'db_status = \'D\' '\
    stmt = ibm_db.prepare(conn, sql)
    ibm_db.execute(stmt)
    tuple = ibm_db.fetch_tuple(stmt)
    while tuple != False:
        print("The ID is : ", tuple[0])
        print("The alias name is : ", tuple[3])
        db_name = tuple[2]
        db_list.append(db_name)
        tuple = ibm_db.fetch_tuple(stmt)
    print(db_list)

    tLock.release()
    print ("db_list has completed lock released")
    global_db_list = db_list

def run_sql (db_name):
    global global_fifo_q
    str = open('C:/Users/sxk11/PycharmProjects/Safari_1/test.txt', 'r').read()
    #print (db_name)
    try:
        conn = ibm_db.connect(db_name, 'tu01945', str)
    except:
        print ("NO connection to :" + db_name)
        #print(ibm_db.stmt_errormsg())
        #print(ibm_db.stmt_error())
        if (ibm_db.conn_error()):
            print("conn_error:")
            print(ibm_db.conn_error())
            while True:
                time.sleep(5)
                try:
                    conn = ibm_db.connect(db_name, 'tu01945', str)
                    if(conn):
                        print("Connection reconnected to :" + db_name)
                        break
                except:
                    print("NO connection to :" + db_name)

            #Send Notification
            #Retry
    else:
        print ("The connection to " + db_name + " was successful!")
        #print (conn)

    sql = 'select snapshot_timestamp, db_name, catalog_partition  from sysibmadm.snapdb'

    #time.sleep(30)

    while True:
        try:
            stmt = ibm_db.exec_immediate(conn, sql)
            #stmt = ibm_db.prepare(conn, sql)
            #ibm_db.execute(stmt)
        except:
            print("Transaction couldn't be completed:")
            print (ibm_db.stmt_errormsg())
            print (ibm_db.stmt_error())
            if (ibm_db.conn_error()):
                print("conn_error:")
                print(ibm_db.conn_error())
                while True:
                    time.sleep(5)
                    try:
                        conn = ibm_db.connect(db_name, 'tu01945', str)
                        if (conn):
                            print("Connection reconnected to :" + db_name)
                            break
                    except:
                        print("NO connection to :" + db_name)

        else:
            tuple = ibm_db.fetch_tuple(stmt)
            while tuple != False:
                #print("The time is : ", tuple[0])
                #print("The catalog name is : ", tuple[1])
                #print("The catalog partition is : ", tuple[2])
                #Store Data in queue
                data = (tuple[1], tuple[0], tuple[2])
                global_fifo_q.put(data)
                #Get next
                tuple = ibm_db.fetch_tuple(stmt)
                time.sleep(10)

        #finally:
        #    ibm_db.close(conn)

class ConnectDB:
    def __init__(self, target_db, db_user):
        self.target_db = target_db
        self.db_user = db_user
        self.db_pass = open('C:/Users/sxk11/PycharmProjects/Safari_1/test.txt', 'r').read()

        self.dbconn = ibm_db.connect(self.target_db, self.db_user, self.db_pass)
        print("Connecting to target...")
        print(self.dbconn)
        #return self.dbconn

    def reconnect(self):
        self.dbconn = ibm_db.connect(self.target_db, self.db_user, self.db_pass)
        print("RE-Connecting to target...")
        print(self.dbconn)
        return self.dbconn


class StoreData2(ConnectDB):
    pass
    def retrieve_and_store(self):
        global global_fifo_q
        while True:
            if not global_fifo_q.empty():
                #Check for active connection
                data_from_q = global_fifo_q.get()
                #Format datatime
                db2_datetime = data_from_q[1].strftime('%Y-%m-%d-%H.%M.%S.%f')
                #print (db2_datetime)
                #print(data_from_q)
                try:
                    stmt = ibm_db.prepare(self.dbconn, "insert into mrj3017.python_test1 (col1, col2, col3) values (?,?,?)")
                    ibm_db.execute(stmt, (data_from_q[0], db2_datetime, data_from_q[2],))
                except:
                    print("Transaction couldn't be completed:")
                    print(ibm_db.stmt_errormsg())
                    print(ibm_db.stmt_error())
                    if (ibm_db.conn_error()):
                        print("conn_error, will retry after 5 seconds:")
                        print(ibm_db.conn_error())
                        while True:
                            time.sleep(5)
                            try:
                                self.dbconn = ibm_db.connect(self.target_db, self.db_user, self.db_pass)
                                if (self.dbconn):
                                    print("Connection reconnected to :" + self.target_db)
                                    break
                            except:
                                print("NO connection to :" + self.target_db)


def Main():

    t1 = threading.Thread(target=get_db_list, name = "Get db list", args=("ADM1P","tu01945",))
    t1.start()
    t1.join()

    print (global_db_list)

    #for db_name in global_db_list:
    #    print (db_name)

    for db_name in global_db_list:
        t2 = threading.Thread(target=run_sql, name = db_name ,args=(db_name,))
        t2.start()
        #t2.join()

    #t3 = threading.Thread(target=get_q_data(), name = 'Retrieve Q Data')
    #t3.start()

    s = StoreData2("ADM1P", "tu01945")
    st = threading.Thread(target=s.retrieve_and_store, name = 'Retrieve Q Data', args=())
    st.start()



    t2.join()
    #t3.join()
    print ("Main completeted")

if __name__ == '__main__':
    Main()
