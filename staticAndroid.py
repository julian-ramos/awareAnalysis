import pickle
import os
import urllib2
import re

'''
This set of functions provides static information like
the category of a given app in googlePlay
'''
     

def getCat(appName):
    '''
    This method finds the category of an app in the google play market
    when it is not already stored in its own file (categoriesLib.dat)
    Also, it updates this file when a new app is observed
    '''
    
    #First load the dictionary where the apps have been stored
    listOfFiles=os.listdir('.')
    
    if 'categoriesLib.dat' in listOfFiles:
        file=open('categoriesLib.dat','rb')
        categsLoader=pickle.load(file)
        file.close()
    else:
        categsLoader={}
        
    if appName in categsLoader.keys():
        return categsLoader[appName]
    else:
        target = 'https://play.google.com/store/apps/details?id=%s&hl=en' % (appName)
        try:
            html = urllib2.urlopen(target).read()
            pat = r'<span itemprop="genre">(.*?)</span>'
            cate = re.findall(pat, html)
            cate=cate[0]
            
            categsLoader[appName]=cate
            file=open('categoriesLib.dat','wb')
            pickle.dump(categsLoader, file)
            file.close()
            
            return cate
        
        except urllib2.HTTPError:
            cate='Unknown'
            categsLoader[appName]=cate
            file=open('categoriesLib.dat','wb')
            pickle.dump(categsLoader, file)
            file.close()
            return 'Unknown'



    
if __name__=='__main__':
    print(getCat('com.pixelberrystudios.hwuandroid'))