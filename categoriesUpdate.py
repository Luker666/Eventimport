import urllib3
import xmltodict
from bs4 import BeautifulSoup
import json
import pymongo
from datetime import datetime


category_url = "http://www.linztermine.at/schnittstelle/downloads/categories_xml.php"

http = urllib3.PoolManager()
response = http.request('GET', category_url)
soup = BeautifulSoup(response.data, "xml")
soup_string = str(soup)

jsonCategories = xmltodict.parse(soup_string)
categories = jsonCategories['catlist']['category']
categories_string = json.dumps(categories)
categories_string_replaced = categories_string.replace("@id","id")
categories_string_replaced = categories_string_replaced.replace("@name","name")
#print(categories_string)
category_obj = json.loads(categories_string_replaced)

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["finzDB"]
categoryDB = mydb["categories"]

categoryDB.drop()
x = categoryDB.insert_many(category_obj)

for obj in categoryDB.find():
        categoryDB.update({'_id':obj['_id']},{'$set':{'lastupdate' : datetime.now()}})