import psycopg2
import psycopg2.extras
import time



class PGConnectDB:
    def __init__(self, dsn_hostname, dsn_port, dsn_database, dsn_uid, dsn_pwd):
        self.dsn_hostname = dsn_hostname
        self.dsn_port = dsn_port
        self.dsn_database = dsn_database
        self.dsn_uid = dsn_uid
        self.dsn_pwd = dsn_pwd

        # Error handing needed for connection.
        try:
            conn_string = "host=" + self.dsn_hostname + " port=" + self.dsn_port + " dbname=" + self.dsn_database + " user=" + self.dsn_uid + " password=" + self.dsn_pwd
            print("Connecting to database\n  ->%s" % (conn_string))
            self.conn = psycopg2.connect(conn_string)
            print("Connected!\n")
        except:
            print("Unable to connect to the database.")
            print("conn_error, will retry after 5 seconds:")
            while True:
                time.sleep(5)
                try:
                    conn_string = "host=" + self.dsn_hostname + " port=" + self.dsn_port + " dbname=" + self.dsn_database + " user=" + self.dsn_uid + " password=" + self.dsn_pwd
                    print("Connecting to database\n  ->%s" % (conn_string))
                    self.conn = psycopg2.connect(conn_string)
                    print("Connected!\n")
                    if (self.conn):
                        print("Connection reconnected to :" + self.dsn_database)
                        break
                except:
                    print("NO connection to :" + self.dsn_database)