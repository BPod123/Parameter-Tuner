import sqlite3
import os
import pandas as pd


class DBHandler(object):
    def __init__(self, targetName, storageDir, colTypes: list):
        self.targetName = targetName
        self.storageDir = storageDir
        self.tableName = "myTable"
        self.columns = [col[0] for col in colTypes]
        self.colTypes = {col[0]: col[1] for col in colTypes}
        self.colIntTextMap = {}
        self.dbPath = os.path.join(self.storageDir, "Results.db")
        for col in self.colTypes:
            if isinstance(self.colTypes[col], dict):
                self.colIntTextMap[col] = self.colTypes[col]
                self.colTypes[col] = "TEXT"

        self.instanceNum = 0
        # self.dbPath = os.path.join(storageDir, "{0}.db".format(self.targetName))
        self.dbInstances = []

    def newInstance(self):
        self.instanceNum += 1
        dbPath = os.path.join(self.storageDir, "db{0}.db".format(self.instanceNum))
        instance = DBInstance(dbPath, self.tableName, self.columns, self.colTypes, self.colIntTextMap)
        # self.dbInstances.append(instance)
        return instance

    def mergeInstances(self):
        """
        Called at the end. Should not have to worry about locks because everything else is done
        """
        con = sqlite3.connect(self.dbPath)
        cursor = con.cursor()
        createString = createTableString(self.tableName, self.columns, self.colTypes)
        cursor.execute(createString)
        for fileName in os.listdir(self.storageDir):
            filePath = os.path.join(self.storageDir, fileName)
            if filePath == self.dbPath or not fileName.endswith(".db"):
                continue
            dbPath = filePath
        # while len(self.dbInstances) > 0:
        #     instance = self.dbInstances.pop(0)
        #     dbPath = instance.dbPath
            baseName = os.path.basename(dbPath)
            dbName = baseName[:baseName.index(".")]
            cursor.execute("ATTACH DATABASE '{0}' AS {1}".format(dbPath, dbName))

            # Copy data over
            copyString = \
                """
                INSERT INTO {0}
                SELECT * FROM '{1}'.{2};
                """.format(self.tableName, dbName, 'myTable')
            cursor.execute(copyString)


            # try:
            #     # Copy data over
            #     copyString = \
            #         """
            #         INSERT INTO {0}
            #         SELECT * FROM '{1}'.{2};
            #         """.format(self.tableName, dbName, self.targetName)
            #     cursor.execute(copyString)
            # except:
            #     try:
            #         # Copy data over
            #         copyString = \
            #             """
            #             INSERT INTO {0}
            #             SELECT * FROM '{1}'.{2};
            #             """.format(self.tableName, dbName, 'myTable')
            #         cursor.execute(copyString)
            #     except:
            #         continue

            con.commit()
            # Detach and remove database
            cursor.execute("DETACH DATABASE {0};".format(dbName))
            # oldDBPaths.append(dbPath)
            os.remove(dbPath)

        # Get top tests
        cursor.close()
        con.close()


    def generateSummary(self, targetName, findMin: bool, limit=None, append=True):
        con = sqlite3.connect(self.dbPath)
        query = "SELECT * FROM " + self.tableName + " ORDER BY {0} ".format(targetName)
        query += "DESC" if not findMin else "ASC"
        if limit is not None:
            query += " LIMIT {0}".format(int(limit))
        query += ";"
        df = pd.read_sql_query(query, con)
        dfPath = os.path.join(self.storageDir, "{0} Top Results.csv".format(self.targetName))
        header = not os.path.isfile(dfPath) or not append
        if append:
            df.to_csv(dfPath, index=False, header=header, mode='a')
        else:
            df.to_csv(dfPath, index=False, header=header)
        con.close()


class DBInstance(object):
    def __init__(self, dbPath, tableName, columns, colTypes, colIntTextMap):
        self.dbPath = dbPath
        self.tableName = tableName
        self.columns = columns
        self.colTypes = colTypes
        self.colIntTextMap = colIntTextMap
        self.readyToMerge = False
        self.created = os.path.exists(dbPath) and os.path.isfile(dbPath)

    def __repr__(self):
        return str(self.dbPath)

    def formatType(self, col, val):
        if self.colTypes[col] == "INTEGER":
            return int(val)
        elif self.colTypes[col] == "REAL":
            return float(val)
        elif self.colTypes[col] == "TEXT":
            # If the type is text,
            if type(val) != str:
                return self.colIntTextMap[col][int(val)]
            else:
                if val.startswith('\'') and val.endswith('\''):
                    return val
                else:
                    return "'" + val + "'"


    def addRowToDB(self, dataDict):
        con = sqlite3.connect(self.dbPath)
        cursor = con.cursor()
        if not self.created:
            # Create the table
            createString = createTableString(self.tableName, self.columns, self.colTypes)
            cursor.execute(createString)
            self.created = True
        insertString = """INSERT INTO {0} ({1})
        VALUES({2});""".format(self.tableName, ",".join(self.columns),
                               ",".join([str(self.formatType(col, dataDict[col])) for col in self.columns]))
        cursor.execute(insertString)
        con.commit()
        cursor.close()
        con.close()


def createTableString(tableName, columns, colTypes):
    # Create the table
    createString = """CREATE TABLE IF NOT EXISTS {0} (
                """.format(tableName)
    for i in range(len(columns)):
        col = columns[i]
        createString += "{0} {1}".format(col, colTypes[col])
        if i < len(columns) - 1:
            createString += ",\n"
        else:
            createString += ");"
    return createString
