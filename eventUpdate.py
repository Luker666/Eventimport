import urllib3
import xmltodict
from bs4 import BeautifulSoup
import json
import pymongo
from datetime import datetime, timedelta
from dateutil import tz


#Veranstaltungen – Standorte: https://www.data.gv.at/katalog/dataset/bb596b23-4096-4d65-861c-a94e7cebf7e2 (Deprecated)
#Linztermine – Orte: https://www.data.gv.at/katalog/dataset/3cca23c2-2aa6-4421-96db-8b914de62d56
#VeranstalterInnen: https://www.data.gv.at/katalog/dataset/9c0a65e3-db8c-4784-98df-b856a9cd3576
#Linztermine - Übersicht Veranstaltungen: https://www.data.gv.at/katalog/dataset/dfa2ff35-d2c4-4196-9989-a1bdbeabbfed
#Linztermine - Übersicht Kategorien: https://www.data.gv.at/katalog/dataset/bb19a103-ef77-4fa9-b3ad-82d76a85df2c

event_url = "http://www.linztermine.at/schnittstelle/downloads/events_xml.php"

http = urllib3.PoolManager()
response = http.request('GET', event_url)
soup = BeautifulSoup(response.data, "xml")
#print(soup)
soup_string = str(soup)
#print(soup_string)

jsonEvents = xmltodict.parse(soup_string)
events = jsonEvents['events']['event']
event_string = json.dumps(events)
event_string_replaced = event_string.replace("@firstdate","firstdate")
event_string_replaced = event_string_replaced.replace("@lastdate","lastdate")
event_string_replaced = event_string_replaced.replace("@id","id")

#print (event_string_replaced)
event_obj = json.loads(event_string_replaced)
#print(event_obj)
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["finzDB"]
eventsDB = mydb["events"]

from_zone = tz.gettz('CET')
to_zone = tz.gettz('UTC')
utc = datetime.utcnow()
utc = utc.replace(tzinfo=from_zone)
cet = utc.replace(tzinfo=to_zone)

timediff = cet - utc #datetime.utcnow()
hours = timediff.seconds/3600
#print (hours)

# tbd if json data available drop and update else do nothing
eventsDB.drop()
x = eventsDB.insert_many(event_obj)
# maybe logtotextfile eventcount with datetime
for obj in eventsDB.find():
        eventsDB.update({'_id':obj['_id']},{'$set':{'lastupdate' : datetime.now()}})

for obj in eventsDB.find():
        if obj['firstdate']:
            if type(obj['firstdate']) is not datetime:
                time = datetime.strptime(obj['firstdate'],'%Y-%m-%d %H:%M:%S')
                eventsDB.update({'_id':obj['_id']},{'$set':{'firstdate' : time - timedelta(hours=hours)}})

for obj in eventsDB.find():
        if obj['lastdate']:
            if type(obj['lastdate']) is not datetime:
                time = datetime.strptime(obj['lastdate'],'%Y-%m-%d %H:%M:%S')
                eventsDB.update({'_id':obj['_id']},{'$set':{'lastdate' : time - timedelta(hours=hours)}})

for obj in eventsDB.find():
        #print(len(obj['date'])/2)
        #print(obj)

        if len(obj['date']) > 2:
            count = 0
            for o in (obj['date']):
                #print (o)
                #print(o['@dFrom'])
                #print(o['@dTo'])
                #print(len(obj['date']))

                if type(obj['date'][count]['@dFrom']) is not datetime:
                    timeFromArr = datetime.strptime(obj['date'][count]['@dFrom'],'%Y-%m-%d %H:%M:%S')
                    eventsDB.update({'_id':obj['_id']},{'$set':{'date.'+str(count)+'.@dFrom' : timeFromArr - timedelta(hours=hours)}})
                if type(obj['date'][count]['@dTo']) is not datetime:
                    timeFromArr = datetime.strptime(obj['date'][count]['@dTo'],'%Y-%m-%d %H:%M:%S')
                    eventsDB.update({'_id':obj['_id']},{'$set':{'date.'+str(count)+'.@dTo' : timeFromArr - timedelta(hours=hours)}})


        if len(obj['date']) < 2 and len(obj['date']) > 0:
            print(obj['@dFrom'])
            print(obj['@dTo'])
            if type(obj['date']['@dFrom']) is not datetime:
                timeFrom = datetime.strptime(obj['date'][count]['@dFrom'], '%Y-%m-%d %H:%M:%S')
                eventsDB.update({'_id': obj['_id']},
                                {'$set': {'date.@dFrom': timeFrom - timedelta(hours=hours)}})
            if type(obj['date']['@dTo']) is not datetime:
                timeTo = datetime.strptime(obj['date'][count]['@dTo'], '%Y-%m-%d %H:%M:%S')
                eventsDB.update({'_id': obj['_id']},
                                {'$set': {'date.@dTo': timeTo - timedelta(hours=hours)}})


for obj in eventsDB.find():
        if(str(obj['firstdate'])[0:-9] != str(obj['lastdate'])[0:-9]):
            #print("Von: " + str(obj['firstdate'].strftime('%d.%m.%Y, %H:%M')) + " Bis: " + str(obj['lastdate'].strftime('%d.%m.%Y, %H:%M')))
            eventsDB.update({'_id': obj['_id']}, {'$set': {'datumstring': "Zeitraum: " + str((obj['firstdate'] + timedelta(hours=hours)).strftime('%d.%m.%Y, %H:%M')) + " - " + str((obj['lastdate'] + timedelta(hours=hours)).strftime('%d.%m.%Y, %H:%M'))}})
        else:
            #print ("Datum: " + obj['firstdate'].strftime('%d.%m.%Y, %H:%M') + " - " + str(obj['lastdate'])[-8:-3])
            eventsDB.update({'_id': obj['_id']}, {'$set': {'datumstring': "Datum: " + str((obj['firstdate'] + timedelta(hours=hours)).strftime('%d.%m.%Y, %H:%M')) + " - " + str(obj['lastdate'] + timedelta(hours=hours))[-8:-3]}})


locations_url = "http://www.linztermine.at/schnittstelle/downloads/locations_xml.php"
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


for site in sites_obj:
    #print(site)
    #cursor = eventsDB.find({'location.id': site['id']})
    #for document in cursor:
    #    print(document)
    #(eventsDB.find({'location.0.id': site['id']}))
    results = eventsDB.find({'location.id': site['id']})
    results_count = results.count()
    #print(results_count)

    i=0
    while i<results_count:
        x = eventsDB.update({'location.id': site['id']},{'$set': {'location.id': site['subof']}}, upsert=True)
        i+=1
        #eventsDB.update({'location.id': site['id']},{'$set': {'location.id': site['subof']}}, upsert=True)
        #print(x)


