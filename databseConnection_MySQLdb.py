import dataOps as dO
import MySQLdb
import ConfigParser


def connectToDatabase(config):
    user = config.get('sql_info', 'user')
    password = config.get('sql_info', 'password')
    host = config.get('sql_info', 'host')
    database = config.get('sql_info', 'database')
    cnx = MySQLdb.connect(user=user, passwd=password,
                          host=host,
                          db=database)
    return cnx


def disconnectFromDatabase(cnx):
    cnx.close()


def query(queryText, cnx):
    cursor = cnx.cursor()
    cursor.execute(queryText)
    data = cursor.fetchall()
    return data


def getColumnsInfo(cnx, config, tablename):
    database = config.get('sql_info', 'database')
    queryText = 'SHOW COLUMNS FROM %s.%s' % (database, tablename)
    info = query(queryText, cnx)
    info = [i[0:2] for i in info]
    return info


def getColumns(cnx, config, tablename):
    database = config.get('sql_info', 'database')
    queryText = 'SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA="%s" AND TABLE_NAME="%s"' % (database, tablename)
    info = query(queryText, cnx)
    info = [i[0] for i in info]
    return info


def getUsers(cnx, tablename):
    queryText = 'SELECT DISTINCT device_id FROM %s ' % (tablename)
    usrsList = query(queryText, cnx)
    usrsList = [i[0] for i in usrsList]
    return usrsList


def getTables(cnx):
    queryText = 'SHOW TABLES'
    tablesList = query(queryText, cnx)
    tablesList = [i[0] for i in tablesList]
    return tablesList


def getIds(cnx, tablename):
    queryText = 'SELECT DISTINCT _id from %s' % (tablename)
    idList = query(queryText, cnx)
    idList = [i[0] for i in idList]
    return idList


def getApps(cnx, tablename):
    queryText = 'SELECT DISTINCT package_name FROM %s' % (tablename)
    idList = query(queryText, cnx)
    idList = [i[0] for i in idList]
    return idList

def getCatsFromTimes(timestampStart, timestampEnd, usr, cnx, tablename):
    '''
    This method returns the categories of the applications used by a user
    between the given timestamps it assumes the table contains the category
    field
    '''
    queryText = 'SELECT timestamp, double_end_timestamp, category FROM %s' % (tablename) + ' WHERE device_id="%s" AND timestamp BETWEEN %f AND %f' % (usr, timestampStart, timestampEnd)
    data = query(queryText, cnx)
    header = 'timestamp, double_end_timestamp, category'.split(',')
    return data, header


def copyTable(cnx, tablename, newtable):
    queryText = 'SHOW CREATE TABLE %s' % (tablename)
    strQuery = query(queryText, cnx)
    ind = strQuery[0][1].find(tablename)
    newQuery = strQuery[0][1][0:ind] + newtable + \
        strQuery[0][1][ind + len(tablename):]
    autoIncrementQuery = 'ALTER TABLE %s auto_increment=1' % (newtable)
    try:
        query(newQuery, cnx)
        cnx.commit()
        query(autoIncrementQuery, cnx)
        cnx.commit()
        return True
    except:
        return False


def colsLabel2Index(cnx, config):
    '''
    This function returns a dictionary that takes the label
    of a given column and returns its index
    '''

    temp = getColumns(cnx, config)
    header = dO.list2Dict(temp)
    return header


if __name__ == "__main__":
    config = ConfigParser.ConfigParser()
    config.read('config.ini')
    tablename = 'applications_history'
    cnx = connectToDatabase(config)
    info = getColumnsInfo(cnx, config, tablename)
    copyTable(cnx, config, 'copied_table')
