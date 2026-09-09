from discord.ext import commands
import math
from datetime import datetime, timedelta
from pymongo import UpdateOne
from datetime import datetime

from cogs import config
from tools import map_api, mongodb_batch_processor

class GeoFSMonitor(commands.Cog):
    def __init__(self, bot, Database, BotLogger):
        self.bot = bot
        self.mongo_db_database = Database
        self.config = config.Config(self.bot)
        self.logger = BotLogger

        self.current_online_users = []

        self.map_api = map_api.MapAPI()
        self.map_api.disableResponseList()
        self.setup_batch_processors(self.mongo_db_database)

        self.MAX_REQUESTS = 10

    def setup_batch_processors(self, db):
        self.batch_processors = {
            "users": mongodb_batch_processor.MongoBatchProcessor(db["users"], self.logger)
        }
        

    def calculate_aircraft_change(self, old_lat, old_lon, new_lat, new_lon): # calculates the distance between the old and new pilot position
            if None in (old_lat, old_lon, new_lat, new_lon):
                return 0
            # convert points to radians
            lon1, lat1, lon2, lat2 = map(math.radians, [old_lon, old_lat, new_lon, new_lat])
    
            # harversine formula
            dlon = lon2 - lon1
            dlat = lat2 - lat1
            a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
            # radius of earth
            R = 6371
            return c * R

    def process_users(self):
        try:
            self.bundled_events = []
            raw = self.map_api.getUsers(False) or []
            if raw == []:
                self.logger.warning("Map API returned no data. Skipping cycle.")
                return self.bundled_events
            
            seen = set(); unique = []
            for u in raw:
                uid = u.userInfo['id']
                if uid and uid not in seen:
                    seen.add(uid); unique.append(u)
            self.current_online_users = unique

            db = self.mongo_db_database
            user_coll = db['users']
            configs = self.config.load_config()

            # prepare existing map
            cur_ids = [u.userInfo['id'] for u in unique]
            exist_map = {d['accountID']: d for d in user_coll.find({'accountID':{'$in':cur_ids}})}

            # Handle users going offline
            going_offline = list(user_coll.find({
                'Online': True,
                'accountID': {'$nin': cur_ids}
            }))
            for doc in going_offline:
                if datetime.now() - doc['lastOnline'] > timedelta(minutes=1):
                    self.logger.debug(f"Account ID: {doc['accountID']} is offline.")
                    evt = {'eventType':'offline', 'timestamp':doc['lastOnline']}
                    self.batch_processors['users'].add_to_batch(
                        UpdateOne(
                            {'accountID':doc['accountID']},
                            {'$set':{'Online':False, 'lastOnline':datetime.now()}, '$push':{'events':evt}}
                        )
                    )
                    self.bundled_events.append({'type': 'activity_change','data': {'acid':doc['accountID'],'status': "offline"}})
            # handle users going online
            going_online = list(user_coll.find({
                'Online': False,
                'accountID': {'$in': cur_ids}
            }))
            for doc in going_online:
                self.logger.debug(f"Account ID: {doc['accountID']} is online.")
                evt = {'eventType':'online', 'timestamp':datetime.now()}
                self.batch_processors['users'].add_to_batch(
                    UpdateOne(
                        {'accountID':doc['accountID']},
                        {'$set':{'Online':True}, '$push':{'events':evt}}
                    )
                )
                if configs['displayActivityChanges']:
                    self.bundled_events.append({'type': 'activity_change', 'data':{'acid':doc['accountID'],'status': "online"}})

            # Process current online users
            for u in unique:
                uid = u.userInfo['id']; cs = u.userInfo['callsign']; ac = u.aircraft['type']; pos = u.coordinates
                self.logger.debug(f"New account detected: {uid} with callsign {cs}.")
                if uid not in exist_map and configs['displayNewAccounts']:
                    self.bundled_events.append({'type': 'new_account', 'data':{'acid':uid,'callsign':cs}})

                # event detection
                evts = []
                # teleport
                old = exist_map.get(uid, {}).get('lastPosition')
                if old:
                    dist = self.calculate_aircraft_change(old[0], old[1], pos[0], pos[1])
                    self.logger.debug(f"Account ID: {uid} teleported {round(dist)} km.")
                    if dist >= 50:
                        evts.append({'eventType':'teleportation','oldLatitude':old[0],'oldLongitude':old[1],'newLatitude':pos[0],'newLongitude':pos[1],'timestamp':datetime.now(),'distance':dist})
                        if configs["displayTeleporations"]:
                            self.bundled_events.append({'type': 'teleportation', 'data':{'acid': uid, 'oldLatitude':old[0],'oldLongitude':old[1],'newLatitude':pos[0],'newLongitude':pos[1],'timestamp':datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'distance':dist}})
                # aircraft change
                old_ac = exist_map.get(uid, {}).get('currentAircraft')
                if old_ac and ac != old_ac:
                    self.logger.debug(f"Aircraft change: {uid} from {old_ac} to {ac}")
                    evts.append({'eventType':'aircraftChange','oldAircraft':old_ac,'newAircraft':ac,'timestamp':datetime.now()})
                    if configs['displayAircraftChanges']:
                        self.bundled_events.append({'type': 'aircraft_change', 'data':{'acid':uid,'oldAircraft':old_ac,'newAircraft':ac}})

                # callsign change
                old_cs = exist_map.get(uid, {}).get('currentCallsign')
                if old_cs and old_cs != cs:
                    self.logger.debug(f"Callsign change: {uid} from {old_cs} to {cs}")
                    evts.append({'eventType':'callsignChange','oldCallsign':old_cs,'newCallsign':cs,'timestamp':datetime.now()})
                    if configs['displayCallsignChanges']:
                        self.bundled_events.append({'type': 'callsign_change', 'data':{'acid':uid,'oldCallsign':old_cs,'newCallsign':cs}})

                # upsert user docllsign':old_cs,'newCallsign':cs
                upsert = UpdateOne(
                    {'accountID':uid},
                    {
                        '$setOnInsert':{'accountID':uid},
                        '$set':{'currentCallsign':cs,'currentAircraft':ac,'Online':True,'lastOnline':datetime.now(),'lastPosition':pos},
                        '$addToSet':{'pastCallsigns':cs},
                        **({'$push':{'events':{'$each':evts}}} if evts else {})
                    },
                    upsert=True
                )
                self.batch_processors['users'].add_to_batch(upsert)

            self.batch_processors['users'].flush_batch()
            return self.bundled_events
        except Exception as e:
            self.logger.error(f"Network or database error in process_users: {e}")
            return []