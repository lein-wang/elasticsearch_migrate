#!/usr/bin/python
#coding:utf-8
'''
    Export and Import ElasticSearch Data.
    Simple Example At __main__
    @author: wgzh159@163.com
    @note:  uncheck consistency of data, please do it by self
'''


import json
import os
import sys
import time
import urllib2

reload(sys)
sys.setdefaultencoding('utf-8')  # @UndefinedVariable

class exportEsData():
    size = 10000
    def __init__(self, url,index,type,target_index):
        self.url = url+"/"+index+"/"+type+"/_search"
        self.index = index
        self.type = type
	self.target_index = target_index #替换原有的index
	self.file_name = self.target_index+"_"+self.type+".json"
    def exportData(self):
        print("export data begin...\n")
        begin = time.time()
        try:
            os.remove(self.file_name)
        except:
            os.mknod(self.file_name)
        msg = urllib2.urlopen(self.url).read()
        print(msg)
        obj = json.loads(msg)
        num = obj["hits"]["total"]
        start = 0
        end =  num/self.size+1
        while(start<end):
            msg = urllib2.urlopen(self.url+"?from="+str(start*self.size)+"&size="+str(self.size)).read()
            self.writeFile(msg)
            start=start+1
        print("export data end!!!\n total consuming time:"+str(time.time()-begin)+"s")
    def writeFile(self,msg):
        obj = json.loads(msg)
        vals = obj["hits"]["hits"]
        try:
            f = open(self.file_name,"a")
            for val in vals:
		#prepare for bulk insert，注意格式
		meta_json = {"index": {"_index": self.target_index, "_type": val["_type"], "_id": val["_id"]}}
		val_json = val["_source"]
                m = json.dumps(meta_json,ensure_ascii=False)
                v = json.dumps(val_json,ensure_ascii=False)
                f.write(m+"\n")
                f.write(v+"\n")
        finally:
            f.flush()
            f.close()

class importEsData():
    def __init__(self,url,index,type):
        self.url = url
        self.index = index
        self.type = type
        self.file_name = self.index+"_"+self.type+".json"
    def importData(self):
        print("import data begin...\n")
        begin = time.time()
        try:
	    s = os.path.getsize(self.file_name)
            f = open(self.file_name,"r")
	    data = f.read(s)
	    #此处有坑: 注意bulk操作需要的格式(以\n换行)
	    self.post(data)
                
        finally:
            f.close()
        print("import data end!!!\n total consuming time:"+str(time.time()-begin)+"s")
    def post(self,data):
	print data
	print self.url
        req = urllib2.Request(self.url,data)
        r = urllib2.urlopen(req)
	response = r.read()
	print response
	r.close()

if __name__ == '__main__':
    '''
        Export Data
        e.g.
                            URL                    index        type
        exportEsData("http://10.100.142.60:9200","watchdog","mexception").exportData()
        
        export file name: watchdog_mexception.json
    '''
    #exportEsData("http://10.100.142.60:9200","watchdog","mexception").exportData()
    exportEsData("http://127.0.0.1:9200","forum","CHAT","chat").exportData()
    exportEsData("http://127.0.0.1:9200","forum","TOPIC","chat").exportData()
    
    
    '''
        Import Data
        
        *import file name:watchdog_test.json    (important)
                    "_" front part represents the elasticsearch index
                    "_" after part represents the  elasticsearch type
        e.g.
                            URL                    index        type
        mportEsData("http://10.100.142.60:9200","watchdog","test").importData()
    '''
    #importEsData("http://10.100.142.60:9200","watchdog","test").importData()
    importEsData("http://127.0.0.1:9200/_bulk","chat","CHAT").importData()
    importEsData("http://127.0.0.1:9200/_bulk","chat","TOPIC").importData()
