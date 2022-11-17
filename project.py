import pymongo
from urllib.request import urlopen
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
import json
import dateutil.parser
import time



res = json.loads(urlopen('https://ipinfo.io/json').read().decode())

locat=res['loc'].split(',')
locat[0]=eval(locat[0])
locat[1]=eval(locat[1])



client = MongoClient("mongodb+srv://chouala25:HrrsowoG@cluster0.jbibelc.mongodb.net/test",server_api=ServerApi('1'))

db = client.vls

def get_vlille():
    url = "https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=3000&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion"
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])

def get_vParis():
    url = "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&rows=-1&facet=name&facet=is_installed&facet=is_renting&facet=is_returning&facet=nom_arrondissement_communes"
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])


def get_vrennes():
    url = "https://data.rennesmetropole.fr/api/records/1.0/search/?dataset=etat-des-stations-le-velo-star-en-temps-reel&q=&start=1000&facet=nom&facet=etat&facet=nombreemplacementsactuels&facet=nombreemplacementsdisponibles&facet=nombrevelosdisponibles"   
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])


def get_vlyon():
    url = " https://download.data.grandlyon.com/ws/rdata/jcd_jcdecaux.jcdvelov/all.json?maxfeatures=-1&start=1"   
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])

   


vlilles = get_vlille()

vparis= get_vParis()

vrennes=get_vrennes()

vlyon = get_vlyon()



vlyon_to_insert = [
    {
        '_id': elem.get('fields', {}).get('code_insee'),
        'name': elem.get('fields', {}).get('address'),
        'geometry': {'lat' : elem.get('lat'),'lng' : elem.get('lng')},
        'size' : elem.get('fields',{}).get('main_stands').get('capacity'),
        'source': {
            'dataset': 'lyon', 
            #'id_ext': elem.get('fields', {}).get('stationcode')
        },
        #'tpe': elem.get('fields', {}).get('type', '') == 'AVEC TPE'
    }
    for elem in vlyon
]


    
vparis_to_insert = [
    {
        '_id': elem.get('fields', {}).get('stationcode'),
        'name': elem.get('fields', {}).get('name'),
        'geometry': elem.get('geometry'),
        'size' : elem.get('fields',{}).get('capacity'),
        'source': {
            'dataset': 'paris', 
            #'id_ext': elem.get('fields', {}).get('stationcode')
        },
        #'tpe': elem.get('fields', {}).get('type', '') == 'AVEC TPE'
    }
    for elem in vparis
]

vlilles_to_insert = [
    {
        '_id': elem.get('fields', {}).get('libelle'),
        'name': elem.get('fields', {}).get('nom', '').title(),
        'geometry': elem.get('geometry'),
        'size': elem.get('fields', {}).get('nbvelosdispo') + elem.get('fields', {}).get('nbplacesdispo'),
        'source': {
            'dataset': 'Lille',
            #'id_ext': elem.get('fields', {}).get('libelle')
        },
        'tpe': elem.get('fields', {}).get('type', '') == 'AVEC TPE'
    }
    for elem in vlilles
]


vrennes_to_insert = [
    {
        '_id': elem.get('fields', {}).get('idstation'),
        'name': elem.get('fields', {}).get('nom','').title(),
        'geometry': elem.get('geometry'),
        'size': elem.get('fields', {}).get("nombreemplacementsactuels") ,
        'source': {
            'dataset': 'rennes',
            'id_ext': elem.get('fields', {}).get('idstation')
        },
        'tpe': elem.get('fields', {}).get('etat', '') == 'En fonctionnement'
    }
    for elem in vrennes
]



try: 
    db.stations.insert_many(vlilles_to_insert, ordered=False)
    db.stations.insert_many(vparis_to_insert, ordered=False)
    db.stations.insert_many(vrennes_to_insert,ordered=False)
    db.stations.insert_many(vlyon_to_insert,ordered=False)
    
except:
    
    pass


#----------------------------------------------------------------
#--------------------- client program----------------------------

db.stations.create_index([('geometry', pymongo.GEOSPHERE)],name='geometry_2dsphere')
def get_nearest_stations(lat, lng,nb):
    
    stations = db.stations.find({'geometry': {'$near': {'$geometry': {'type': 'Point','coordinates': [lng, lat]}}}})
    return stations[:nb]

def get_available_stations(stations):

    available_stations = []
    for station in stations:
        if db.datas.find({'station_id': station['_id']}).sort('date', -1):
            available_stations.append(station)
            available_stations.append((db.datas.find({'station_id': station['_id']},{'_id':0,'bike_availbale':1,'stand_availbale':1}).sort('date', -1))[0])
    return available_stations

def get_nearest_available_station(lat, lng, nb_stations):

    stations = get_nearest_stations(lat, lng, nb_stations)
    a=get_available_stations(stations)
    
    print(a[0]['name'],a[1])
    

#----------------------------------------------------------------
#-----------------------search a station-------------------------

db.stations.create_index([('name',pymongo.TEXT)])
search=input("Rechercher le nom d'une station avec quelques lettres")
aa=db.stations.find({'$text':{"$search": "\""+search+"\""}})
longueur=0
for b in aa:
    print(b['name'])
    longueur+=1
    
    
    
    
    
#----------------------------------------------------------------
#- update / delete a station
while longueur!=1:
    longueur=0
    search = input("Affiner votre recherche pour sélectionner une station en particulier")
    aa = db.stations.find({'$text':{"$search": "\""+search+"\""}})

    for b in aa:
        print(b['name'])
        longueur+=1
rep=""
while rep!="u" and rep!="d":
    rep=input("Pressez u pour mettre à jour ou d pour supprimer la station")
if rep=="u":
    vlilles = get_vlille()
    datas = [
        {
            "bike_availbale": elem.get('fields', {}).get('nbvelosdispo'),
            "stand_availbale": elem.get('fields', {}).get('nbplacesdispo'),
            "date": dateutil.parser.parse(elem.get('fields', {}).get('datemiseajour')),
            "station_id": elem.get('fields', {}).get('libelle')
        }
        for elem in vlilles
    ]
    for data in datas:
        aa = db.stations.find({'$text': {"$search": "\"" + search + "\""}})
        for j in aa:
            if data["station_id"]==j['_id']:
                db.datas.update_one({'date': data["date"], "station_id": data["station_id"]}, {"$set": data}, upsert=True)
                print("mise à jour faite")

if rep=="d":
    aa = db.stations.find({'$text': {"$search": "\""+search+"\""}})
    for j in aa:
        db.stations.delete_one({"_id":j['_id']})
        print("suppression faite")



#----------------------------------------------------------------
#- deactivate all stations in an area

# 
def desactivate():
    latmax=float(input("Entrez la latitude max"))
    latmin=float(input("Entrez la latitude min"))
    lonmax=float(input("Entrez la longitude max"))
    lonmin=float(input("Entrez la longitude min"))
    p= db.stations.find({'geometry.coordinates.0':{"$gte":lonmin,"$lte":lonmax},'geometry.coordinates.1':{"$gte":latmin,"$lte":latmax}})
    for i in p:
        db.stations.update_one({'_id':i['_id']},{"$set":{'tpe':False}})

#- give all stations with a ratio bike/total_stand under 20% between 18h and 19h00 (monday to friday)





while True:
    
    print("Votre station la plus proche: ")
    get_nearest_available_station(locat[0], locat[1], 1)
    
    
    
    print('update')
    vparis = get_vParis()
    datas_paris = [
        {
           
            "stand_availbale": elem.get('fields', {}).get('numdocksavailable'),
            "bike_availbale": elem.get('fields', {}).get('numbikesavailable'),
            "date": elem.get('fields', {}).get('record_timestamp'),
            "station_id": elem.get('fields', {}).get('stationcode')
        }
        for elem in vparis
    ]
    
    vlilles = get_vlille()
    datas_lille = [
        {
            "bike_availbale": elem.get('fields', {}).get('nbvelosdispo'),
            "stand_availbale": elem.get('fields', {}).get('nbplacesdispo'),
            "date": dateutil.parser.parse(elem.get('fields', {}).get('datemiseajour')),
            "station_id": elem.get('fields', {}).get('libelle')
        }
        for elem in vlilles
    ]
    
    vrennes = get_vrennes()
    datas_rennes = [
        {
            "bike_availbale": elem.get('fields', {}).get("nombrevelosdisponibles"),
            "stand_availbale": elem.get('fields', {}).get('nombreemplacementsdisponibles'),
            "date": dateutil.parser.parse(elem.get('fields', {}).get('lastupdate')),
            "station_id": elem.get('fields', {}).get('idstation')
        }
        for elem in vrennes
    ]
    
    vrennes = get_vlyon()
    datas_lyon = [
        {
            "bike_availbale": elem.get('fields', {}).get('available_bike_stands'),
            "stand_availbale": elem.get('fields', {}).get('available_bikes'),
            "date": dateutil.parser.parse(elem.get('fields', {}).get('lastupdate')),
            "station_id": elem.get('fields', {}).get('code_insee')
        }
        for elem in vlyon
    ]
    
    
    
    
    for data in datas_paris:
        db.datas.update_one({'date': data["date"], "station_id": data["station_id"]}, { "$set": data }, upsert=True)
    
    for data in datas_lille:
        db.datas.update_one({'date': data["date"], "station_id": data["station_id"]}, { "$set": data }, upsert=True)
    
    for data in datas_rennes:
        db.datas.update_one({'date': data["date"], "station_id": data["station_id"]}, { "$set": data }, upsert=True)
    
    for data in datas_lyon:
        db.datas.update_one({'date': data["date"], "station_id": data["station_id"]}, { "$set": data }, upsert=True)
    

    time.sleep(1)
