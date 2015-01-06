a=raw_input('something')
print(a)

#Redoing the entire app2cats method
    #I want to create an entire copy of applications history in which we have the
    #category information already inserted for that i have to first
    #recreate applications_history structure then create my own table and add the category
    #column, hopefully this is faster than updating a table
    
    if appsHistTable in tablesList:
        #Create the category column
        #Note add a way to check if it already exists
        print('applications_history table found')
        
        columns=dbC.getColumns(cnx, database, 'applications_history')
        
        if 'category' not in columns:
            
            queryText=('alter table applications_history add category varchar(30)')
            dbC.query(queryText, cnx)
            cnx.commit()
            print('category column created on applications_history table')
        else:
            print('category column already in applications_history')
            if update==False:
                print('Update = False')
                print('Not updating applications_history')
                return True
            
            ans=raw_input('Do you want to update? \n')
            if not('y' in ans or 'Y' in ans):
                return True
        
            
#         idList=dbC.getIds(cnx, 'applications_history')
