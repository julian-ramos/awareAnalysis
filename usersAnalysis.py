import dataGraphs as dG
import dataWrite as dW
import staticAndroid as static
import mysql.connector
import databaseConnection as dbC
import dataOps as dO
import numpy as np


def connect2Database():
    cnx=dbC.connectToDatabase('securacy')
    return cnx

def sessionsCategories(cnx,sessionsTable):
    usrList=dbC.getUsers(cnx, sessionsTable)

def sessionsExtract(cnx,scale=1000.0,screenTable='screen',appsHistTable='applications_history',tablename='sessions'):
    '''
    This functions extracts the sessions from a screen table
    The createTable variable indicates whether we want to create 
    a table specific for this data on the database. Check that the user
    specified in databaseConnection has write access to the database
    Scale refers to a number to divide the timestamps in the database
    to get the timestamps in the correct range
    screenTable is the table where there screen state was stored default value is 'screen'
    appsHistTable is the table where applications history was recorded
    tablename is the name of the table to be created
    This function by default always drops any table with the same name as tablename
    '''
    # First it needs to obtain the list of users
#     queryText=('select distinct(device_id) from screen ')
#     usrsList=dbC.query(queryText, cnx)
    usrsList=dbC.getUsers(cnx, screenTable)
    temp=dbC.getColumns(cnx, database, screenTable)
    header=dO.list2Dict(temp)
    sessions=[]
    print('starting sessions extraction')
    for usr in usrsList:
        queryText=('select * from %s where device_id = "%s"'%(screenTable,usr))        
        data=dbC.query(queryText, cnx)
        start=False
        end=False
        print('Extracting session for user %s --progress %.1f'%(usr,1.0*usrsList.index(usr)/len(usrsList)))
        if header['_id']==0:
            #This step makes sure that the data points
            #are sorted so that we can now extract sessions from the data
            #it only works if the first data point is the _id column
            #needs to add a smarter way to find the _id column
            data=sorted(data)
        else:
            print('couldn not sort the data')
            
        for i in range(len(data)):
            ss=data[i][header['screen_status']]
            t=data[i][header['timestamp']]
            id=data[i][header['_id']]
            device_id=data[i][header['device_id']]
            if ss==3:
                start=True
                ts=t
                ids=id
            if start==True and ss==0:
                end=True
                te=t
                ide=id
            if start==True and end==True:
                start=False
                end=False
            #userid,id_start,id_end,timestampStart, timestampEnd, Duration
                #numApps=numAppsxSessions(cnx, ts, te, device_id, appsHistTable,tablename)
                sessions.append([usr,ids,ide,ts,te,(te-ts)/scale])
                
    #Now the sessions data has been extracted we can write it on the database as a new table
    #If the table already exists drop it
    print('creating sessions table')
    queryText=('show tables')
    tablesList=dbC.getTables(cnx)
    
    if tablename in tablesList:
        queryText=('drop table %s'%(tablename))
        dbC.query(queryText, cnx)
        cnx.commit()
        print('%s table was erased!'%(tablename))
    
    queryText=('CREATE TABLE %s (id MEDIUMINT NOT NULL AUTO_INCREMENT, device_id VARCHAR(40), id_start INT ,  id_end INT ,'%(tablename)+ 
               'timestamp_start DOUBLE , timestamp_end DOUBLE , duration FLOAT, PRIMARY KEY (id))')
    dbC.query(queryText, cnx) 
    cnx.commit()
    #Inserting the data into the table
    for i in range(len(sessions)):
        id,ids,ide,ts,te,d,numApps=sessions[i]
        queryText=('INSERT INTO sessions (device_id,id_start,id_end,timestamp_start,timestamp_end,duration,numApps) '+
                   'VALUES ("%s",%d,%d,%.6f,%.6f,%f)'%(id,ids,ide,ts,te,d))
        dbC.query(queryText, cnx)
    cnx.commit()
    print('done inserting data')
    print('session extraction done')
    
def app2cats(cnx,database,appsHistTable,tablename='applications_history2',update=False):
    '''
    This function transforms from applications to categories.
    Categories as defined in the google play market
    This function copies appHisTable and adds the category column
    '''
    
    #Check for the tablename table first
    tablesList=dbC.getTables(cnx)
    
    if tablename in tablesList:
        queryText=('drop table %s'%(tablename))
        dbC.query(queryText, cnx)
        print('%s table already exists dropping!'%(tablename))
        cnx.commit()
        
#     queryText=('create table %s  '%(tablename)+
#                '(_id INT(11), timestamp double, device_id varchar(255), '+
#                'package_name text, application_name text, process_importance int(11),'+
#                'process_id int(11), double_end_timestamp double, is_system_app int(11) )')

    queryText=('create table %s (_id int(11), category text, duration double)'%(tablename))
    dbC.query(queryText, cnx)
    appsList=dbC.getApps(cnx,appsHistTable)

    #The easiest way to do this is by using the _id insert into the sessions table and then 
    #making a join operation on both tables :) when needed
        
    for appId in range(len(appsList)):  
        category=static.getCat(appsList[appId])
        queryText=('select _id,timestamp,double_end_timestamp from %s where package_name="%s"'%(appsHistTable,appsList[appId]))
        data=dbC.query(queryText, cnx)
#         colsInds=dbC.colsLabel2Index(cnx, database, appsHistTable)
#         idIdx=colsInds['_id']
#         rows=[]
        for rowId in range(len(data)):
            temp_id=data[rowId][0]
            ts=data[rowId][1]
            te=data[rowId][2]
            duration=(te-ts)/1000.0
#             rows.append([temp_id,category])
#             dW.writeLoL2csv(rows, 'w',filename='temp.csv')
#             queryText=('load data local infile "/home/julian/awareAnalysis/temp.csv" into table %s.%s fields terminated by "," ' 
#                     %(database,tablename))
            queryText=('insert into %s (_id, category,duration) values(%d, "%s",%f)'%(tablename,temp_id,category,duration))
            dbC.query(queryText, cnx)
        cnx.commit()
        print('category column updated for app %s --%d '%(appsList[appId],100.0*appId/len(appsList)))
        
    print('done')
    return True
    
    
def sessionJoinAppHist(cnx,tablename='tempJoin',appsHistTable='applications_history',sessionsTable='sessions'):
    '''
    This function extracts further statistics from the sessions extracted
    by the sessionExtract method
    '''
    
    #The join between two temporary tables created to extract the applicatiosn history and the sessions is really fast
    #the only thing I have to be careful about is about filtering out system apps and apss according to process importance
    
    usrsList=dbC.getUsers(cnx,'sessions')
    tablesList=dbC.getTables(cnx)
    
    if tablename in tablesList:
        queryText=('drop table %s'%(tablename))
        dbC.query(queryText,cnx)
        print('erasing previous %s table'%(tablename))
#         cnx.commit()
    
    

    
    for usr in usrsList:
        #Erase any temp tables if they exist
        tablesList=dbC.getTables(cnx)
        if 'tempApps' in tablesList:
            queryText=('drop table tempApps')
            dbC.query(queryText,cnx)
            cnx.commit()
            
        if 'tempSess' in tablesList:
            queryText=('drop table tempSess')
            dbC.query(queryText,cnx)
            cnx.commit()
            
        if tablename in tablesList:
#         queryText=('drop table tempJoin')
#         dbC.query(queryText,cnx)
#         cnx.commit()
            tempJoinFlg=True
        else:
            tempJoinFlg=False

        #Create temp applications history table
        queryText=('create table tempApps select * from %s where device_id="%s" '%(appsHistTable,usr)+
                   'and process_importance=100 and is_system_app=0 '+ 
                   'and application_name !="securacy" and application_name !="AWARE"')
        dbC.query(queryText,cnx)
        cnx.commit()
        
        
        #Create temp sessions
        queryText=('create table tempSess select * from %s where device_id="%s"'%(sessionsTable,usr))
        dbC.query(queryText,cnx)
        cnx.commit()
        
        
        if tempJoinFlg==True:
            queryText=('insert ignore into %s select '%(tablename)+
                       'tempApps.device_id, '+
                       'tempApps.timestamp as apps_timestamp_start, '+ 
                       'tempApps.double_end_timestamp as apps_timestamp_end, '+
                       'tempApps.package_name, '+
                       'tempApps.application_name, '+
                       'tempApps._id, '+
                       'tempSess.id as session_id, '+
                       'tempSess.id_start, '+
                       'tempSess.id_end, '+     
                       'tempSess.duration, '+
                       'tempSess.timestamp_start as sess_timestamp_start, '+
                       'tempSess.timestamp_end as sess_timestamp_end '+ 
                       'from tempApps join tempSess on '+
                       'tempApps.timestamp>=tempSess.timestamp_start and '+
                       'tempApps.double_end_timestamp<=tempSess.timestamp_end '
                       )
            count+=1
            print('appending ---progress[%.1f %]'%(100.0*count/len(usrsList)))
            dbC.query(queryText,cnx)
            cnx.commit()
            
        else:
            #Create table with the join of the two tables
            queryText=('create table %s select '%(tablename)+
                       'tempApps.device_id, '+
                       'tempApps.timestamp as apps_timestamp_start, '+ 
                       'tempApps.double_end_timestamp as apps_timestamp_end, '+
                       'tempApps.package_name, '+
                       'tempApps.application_name, '+
                       'tempApps._id, '+
                       'tempSess.id as session_id, '+
                       'tempSess.id_start, '+
                       'tempSess.id_end, '+     
                       'tempSess.duration, '+
                       'tempSess.timestamp_start as sess_timestamp_start, '+
                       'tempSess.timestamp_end as sess_timestamp_end '+ 
                       'from tempApps join tempSess on '+
                       'tempApps.timestamp>=tempSess.timestamp_start and '+
                       'tempApps.double_end_timestamp<=tempSess.timestamp_end '
                       )
            print('creating %s'%(tablename))
            dbC.query(queryText,cnx)
            cnx.commit()
            count=1
            
    
    #last comment
    #Erase old comments
    #check that the join is working fine
    #once that is done we are good to go
        
    #go through each of the users data and create a temporary
    #sessions and applications history table
    #the applications history table should already
    #have filtered out by process importance and by system app
    
    
#     queryText=('select * from %s '%(sessionsTable)+
#                'inner JOIN %s '%(appsHistTable)+  
#                'ON %s.timestamp>=%s.timestamp_start and '%(appsHistTable,sessionsTable)+
#                '%s.double_end_timestamp<=%s.timestamp_end'%(appsHistTable,sessionsTable))    
    
    
def numAppsxSessions(cnx,ts,te,device_id,appsHistTable,sessionsTable):
    '''
    This function finds the apps used between two timestamps
    uses the sessions table generated by sessionsExtract
    '''
    queryText=('select count(distinct(package_name)) from %s where timestamp>=%f and double_end_timestamp<=%f and device_id="%s"'
               %(appsHistTable,ts,te,device_id))
    res=dbC.query(queryText, cnx)
    return res[0][0]

def catsTable(targetTable,tablename,column='package_name',id='session_id'):
    '''
    Creates a categories table using the data from a join table between 
    apps history and sessions. This table must be specified on targetTable
    Column corresponds to the package or application name column. 
    tablename is the name assigned to the output table
    '''
    queryText=('select %s,%s from %s'%(column,session_id,targetTable))
    data=dbC.query(queryText,cnx)
    


    
    
def sessionsStats(cnx):
    '''
    This function executes several statistics on the sessions found
    accross all users
    '''
    
    
    
    
    
    queryText=('select duration from sessions')
    data=np.array(dbC.query(queryText, cnx))
    data=[i[0] for i in data]
    labelX='duration'
    labelY='counts'
    #Filter out sessions that are outliers
    q1=np.percentile(data, 25)
    q3=np.percentile(data, 75)
    IQR=q3-q1
    lowB=q1-IQR*1.5
    upB=q3+IQR*60
    outliers=[i for i in range(len(data)) if data[i]<lowB or data[i]>upB]
    
    
    #Before going forward I have to check why some sessions have such a high value
    #to do that I have to extract first the number of different applications 
    #used per session
    #I should actually cluster sessions based on duration?? and then look at the average 
    #statistics of every cluster?
    
    
    
    
    #Histogram of sessions duration
    dG.histogram(data,labelX,labelY)
    
    #Filter out applications that should not count 
    #Histogram of number of different applications used per session
    #Distribution of sessions for different days of the week
    #Distribution of number of applications used for different days of the week
    #Histogram of session duration for morning, afternoon and evening
    #Histogram of session duration for morning, afterning and evening for different days of the week 
    #It could be a boxplot
    
    
                
            


if __name__=='__main__':
    #Changes to do
    #instead of updating tables
    #Always create new tables and drop if there is an old table
    #in this way if by any chance the code fails it will not in any way
    #affect the stored data
    
    database='securacy'
        
    cnx=connect2Database()
    sessionJoinAppHist(cnx,tablename='sessJoinApps')
    #ONLY NEED TO EXECUTE ONCE
    #WILL DROP TABLES!
#     ans=raw_input('do you really want to rewrite/write the'+
#                 'sessions and categories tables?')
#     if 'y' in ans or 'Y' in ans:
#         app2cats(cnx, database,'applications_history',tablename='categories')
#     sessionsExtract(cnx,tablename='sessions')
    
#     #Setting up longer query times
#     queryText=('SET GLOBAL connect_timeout=1000')
#     dbC.query(queryText, cnx)
#     cnx.commit()
#     
#     queryText=('SET GLOBAL max_allowed_packet=1073741824')
#     dbC.query(queryText, cnx)
#     cnx.commit()


#     ts=1393506492902
#     te=1393506519721
#     appsHistTable='applications_history'
#     id='afbaec11-75c4-48df-b43c-3878b3359a04'
#     print(numAppsxSessions(cnx, ts, te, id,appsHistTable))


#     sessionsStats(cnx)

    cnx.close()
    
    
    
    
    
        