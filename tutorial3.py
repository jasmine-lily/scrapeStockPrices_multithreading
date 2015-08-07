import urllib
import csv
import pymysql
from threading import Thread

class minuteprice:
            
    def __init__(self):
        self.price = [] 
    
    def getlink(self,stock):
        return('https://www.google.com/finance/getprices?q=%s&x=NASD&i=60&p=1d&f=d,c,h,l,o,v' %(stock))
    
    def getprice(self,stock):
        url = self.getlink(stock)
        
        loop, error = 0, 0
        while loop < 3:
            loop += 1
            try:
                r = urllib.urlopen(url)
                rcontent = r.read()
                r.close()                 
            except:
                error += 1
                continue
            break
        
        if error == 3:
            print 'Error for SYMBOL: ' + stock + '!'
        else:
            for row in rcontent.split('\n'):
                if len(row.split(','))==6 and 'COLUMNS' not in row:                  
                    self.price.append(row)
            return self.price
 
                           
class onethread: 
     
    def __init__(self):
        self.pricelib = {}
     
    def connect(self,pwd):
        db = pymysql.connect(host = pwd[0], 
                                 port = int(pwd[1]), 
                                 user = pwd[2], 
                                 passwd = pwd[3], 
                                 db = pwd[4]
                                 ) 
        return db
     
    def vectorize(self,slist,chunk,pwd):  
         
        db = self.connect(pwd)
        cursor = db.cursor()
        tablename = 'price'+str(chunk)
                
        for stock in slist:
            print stock
            self.pricelib[stock] = minuteprice()
            table = self.pricelib[stock].getprice(stock)
            if len(table) == 0:
                pass
            elif len(table) != 0:
                first = table[0]
                start = first.split(',')[0].split('a')[1]
                        
                cursor.execute("INSERT INTO " + tablename + " VALUES (%s,%s,%s,%s,%s,%s,%s)",
                               (stock,
                                int(start),
                                float(first.split(',')[1]),
                                float(first.split(',')[2]),
                                float(first.split(',')[3]),
                                float(first.split(',')[4]),
                                float(first.split(',')[5]))
                               )
                db.commit()
                 
                if len(table) == 1:
                    pass
                else:                   
                    for record in table[1:]:
                        cursor.execute("INSERT INTO " + tablename + " VALUES (%s,%s,%s,%s,%s,%s,%s)",
                                       (stock,
                                        int(start)+60*int(record.split(',')[0]),
                                        float(record.split(',')[1]),
                                        float(record.split(',')[2]),
                                        float(record.split(',')[3]),
                                        float(record.split(',')[4]),
                                        float(record.split(',')[5]))
                                       )
                    db.commit()
                     
                                     
def main():
            
    slist = []
    with open('companylist.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            slist.append(row['Symbol'])
    csvfile.close()
            
    chunks=[slist[x:x+400] for x in xrange(0, len(slist), 400)] 
    print len(chunks)
    
    thlist = []
              
    f = open('databasepassword.txt')
    pwd = f.read().split('\n')    
                
    for i in range(len(chunks)):    
        chunk = onethread() 
        ch = chunks[i]
        t = Thread(target=chunk.vectorize,args=(ch,i,pwd,))
        t.start()
        thlist.append(t)  
                    
    for th in thlist:
        th.join()
      
           
if __name__ =="__main__":
    main()
