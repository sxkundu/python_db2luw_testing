import ibm_db

class DB2ConnectDB:
    def __init__(self, target_db, db_user, db_password):
        self.target_db = target_db
        self.db_user = db_user
        self.db_password = db_password
        #self.db_password = open('C:/Users/sxk11/PycharmProjects/Safari_1/test.txt', 'r').read()

        self.dbconn = ibm_db.connect(self.target_db, self.db_user, self.db_password)
        #print("Connecting to target...")
        #print(self.dbconn)
        #return self.dbconn