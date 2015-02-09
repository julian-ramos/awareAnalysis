import dataOps as dO
import pymysql
import ConfigParser


def colsLabel2Index(cnx, config):
    '''
    This function returns a dictionary that takes the label
    of a given column and returns its index
    '''

    temp = getColumns(cnx, config)
    header = dO.list2Dict(temp)
    return header


def copyTable(cnx, config, newtable):
    tablename = config.get('sql_info', 'tablename')
    queryText = ('show create table %s' % (tablename))
    strQuery = query(queryText, cnx)
    ind = strQuery[0][1].find(tablename)
    newQuery = strQuery[0][1][0:ind] + newtable + \
        strQuery[0][1][ind + len(tablename):]
    autoIncrementQuery = 'alter table %s auto_increment=1' % (newtable)
    try:
        query(newQuery, cnx)
        cnx.commit()
        query(autoIncrementQuery, cnx)
        cnx.commit()
        return True
    except:
        return False


def getCatsFromTimes(timestampStart, timestampEnd, usr, cnx, config):
    '''
    This method returns the categories of the applications used by a user
    between the given timestamps it assumes the table contains the category
    field
    '''
    tablename = config.get('sql_info', 'tablename')
    queryText = ('select timestamp, double_end_timestamp, category from %s' % (tablename) +
                 ' where device_id="%s" and timestamp between %f and %f' % (usr, timestampStart, timestampEnd))
    data = query(queryText, cnx)
    header = 'timestamp, double_end_timestamp, category'.split(',')
    return data, header


def getApps(cnx, config):
    tablename = config.get('sql_info', 'tablename')
    queryText = ('select distinct(package_name) from %s ' % (tablename))
    idList = query(queryText, cnx)
    idList = [i[0] for i in idList]
    return idList


def getIds(cnx, config):
    tablename = config.get('sql_info', 'tablename')
    queryText = ('select distinct(_id) from %s ' % (tablename))
    idList = query(queryText, cnx)
    idList = [i[0] for i in idList]
    return idList


def getTables(cnx):
    queryText = ('show tables')
    tablesList = query(queryText, cnx)
    tablesList = [i[0] for i in tablesList]
    return tablesList


def getUsers(cnx, config):
    tablename = config.get('sql_info', 'tablename')
    queryText = ('select distinct(device_id) from %s ' % (tablename))
    usrsList = query(queryText, cnx)
    usrsList = [i[0] for i in usrsList]
    return usrsList


def getColumns(cnx, config):
    database = config.get('sql_info', 'database')
    tablename = config.get('sql_info', 'tablename')
    queryText = ('SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA="%s" AND TABLE_NAME="%s"'
                 % (database, tablename))
    info = query(queryText, cnx)
    info = [i[0] for i in info]
    return info


def getColumnsInfo(cnx, config):
    database = config.get('sql_info', 'database')
    tablename = config.get('sql_info', 'tablename')
    queryText = ('show columns from %s.%s'
                 % (database, tablename))
    info = query(queryText, cnx)
    info = [i[0:2] for i in info]
    return info


def query(queryText, cnx):
    cursor = cnx.cursor()
    cursor.execute(queryText)
    data = []
    for retrievedData in cursor:
        #         print list(retrievedData)
        data.append(list(retrievedData))
    return data


def connectToDatabase(config):
    user = config.get('sql_info', 'user')
    password = config.get('sql_info', 'password')
    host = config.get('sql_info', 'host')
    database = config.get('sql_info', 'database')
    cnx = pymysql.connect(user=user, password=password,
                          host=host,
                          database=database)
    return cnx


def disconnectFromDatabase(cnx):
    cnx.close()


if __name__ == "__main__":
    config = ConfigParser.ConfigParser()
    config.read('config.ini')
    cnx = connectToDatabase(config)
    info = getColumnsInfo(cnx, config)
    copyTable(cnx, config, 'copied_table')
