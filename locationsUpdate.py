import urllib3
import xmltodict
from bs4 import BeautifulSoup
import json
import pymongo
from datetime import datetime

locations_url = "http://www.linztermine.at/schnittstelle/downloads/locations_xml.php"

httpLocations = urllib3.PoolManager()
responseLocations = httpLocations.request('GET', locations_url)
soupLocations = BeautifulSoup(responseLocations.data, "xml")
soup_stringLocations = str(soupLocations)

jsonLocations = xmltodict.parse(soup_stringLocations)
locations = jsonLocations['loclist']['location']
#print(json.dumps(locations))
locations_string = json.dumps(locations)
#print (locations_string)
locations_string_replaced = locations_string.replace("@id","id")
locations_obj = json.loads(locations_string_replaced)


httpSites = urllib3.PoolManager()
responseSites = httpSites.request('GET', locations_url)
soupSites = BeautifulSoup(responseSites.data, "xml")
soup_stringSites = str(soupSites)
jsonSites = xmltodict.parse(soup_stringSites)
sites = jsonSites['loclist']['site']
#print(json.dumps(sites))
sites_string = json.dumps(sites)
#print (sites_string)
sites_string_replaced = sites_string.replace("@id","id")
sites_obj = json.loads(sites_string_replaced)


myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["finzDB"]
locationsDB = mydb["places"]


#locationsDB.drop()
#x = locationsDB.insert_many(locations_obj)

'''
for obj in eventsDB.find():
        eventsDB.update({'_id':obj['_id']},{'$set':{'lastupdate' : datetime.now()}})
'''


for location in locations_obj:
    locationsDB.update_one({'id': location['id']}, {'$set': {'id': location['id'],
                                                              'name': location['name'],
                                                              'street': location['street'],
                                                              'postcode': location['postcode'],
                                                              'city': location['city'],
                                                              'state': location['state'],
                                                              'telephone': location['telephone'],
                                                              'link': location['link'],
                                                              'lastupdate' : datetime.now(),
                                                              'updated': 1}}, upsert=True)

for site in sites_obj:
    locationsDB.update_one({'id': site['id']}, {'$set': {'id': site['id'],
                                                              'name': site['name'],
                                                              'street': site['street'],
                                                              'postcode': site['postcode'],
                                                              'city': site['city'],
                                                              'state': site['state'],
                                                              'telephone': site['telephone'],
                                                              'link': site['link'],
                                                              'subof': site['subof'],
                                                              'lastupdate' : datetime.now(),
                                                              'updated': 1}}, upsert=True)


# update all events, set location id = parent where site

'''
event_url = "http://www.linztermine.at/schnittstelle/downloads/events_xml.php"

httpEvents = urllib3.PoolManager()
responseEvent = httpEvents.request('GET', event_url)
soupEvent = BeautifulSoup(responseEvent.data, "xml")
#print(soup)
soup_stringEvent = str(soupEvent)

jsonEvents = xmltodict.parse(soup_stringEvent)
events = jsonEvents['events']['event']
event_string = json.dumps(events)
event_string_replaced = event_string.replace("@firstdate","firstdate")
event_string_replaced = event_string_replaced.replace("@lastdate","lastdate")
event_string_replaced = event_string_replaced.replace("@id","id")
eventsDB = mydb["events"]

for site in sites_obj:
    #print(site)
    #cursor = eventsDB.find({'location.id': site['id']})
    #for document in cursor:
    #    print(document)
    #(eventsDB.find({'location.0.id': site['id']}))
    eventsDB.update({'location.id': site['id']},{'$set': {'location.id': site['subof']}}, upsert=True)
'''

