import urllib3
import xmltodict
from bs4 import BeautifulSoup
import json
import pymongo
from datetime import datetime


organizers_url = "http://www.linztermine.at/schnittstelle/downloads/organizers_xml.php"

http = urllib3.PoolManager()
response = http.request('GET', organizers_url)
soup = BeautifulSoup(response.data, "xml")
soup_string = str(soup)

jsonOrganizers = xmltodict.parse(soup_string)
organizers = jsonOrganizers['organizers']['organizer']
#print(json.dumps(locations))
organizers_string = json.dumps(organizers)
#print (organizers_string)
organizers_string_replaced = organizers_string.replace("@id", "id")
organizers_obj = json.loads(organizers_string_replaced)



myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["finzDB"]


# tbd update lat long and change to upsert
organizerDB = mydb["organizers"]


#organizerDB.drop()
#x = organizerDB.insert_many(organizers_obj)


for organizer in organizers_obj:
    organizerDB.update_one({'id': organizer['id']}, {'$set': {'id': organizer['id'],
                                                              'name': organizer['name'],
                                                              'street': organizer['street'],
                                                              'postcode': organizer['postcode'],
                                                              'city': organizer['city'],
                                                              'state': organizer['state'],
                                                              'telephone': organizer['telephone'],
                                                              'link': organizer['link'],
                                                              'lastupdate': datetime.now(),
                                                              'updated': 1}}, upsert=True)



