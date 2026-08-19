from discord.ext import commands
import math
from datetime import datetime, timedelta
from pymongo import UpdateOne
from datetime import datetime
import pandas as pd
import numpy as np

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
        

    def calculate_aircraft_change(self, old_lat, old_lon, new_lat, new_lon):
        lon1, lat1, lon2, lat2 = map(np.radians, [old_lon, old_lat, new_lon, new_lat])
        
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
        c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
        
        R = 6371
        return c * R

    def process_users(self):
        self.bundled_events = []
        raw = self.map_api.getUsers(False) or []
        seen = set()

        # flattens the data
        flattened_current_users = []
        for user in raw:
            acid = user.userInfo['id']
            if acid and acid not in seen:
                seen.add(acid)
                flattened_current_users.append({
                    'accountID': acid,
                    'callsign_current': user.userInfo['callsign'],
                    'aircraft_current': user.aircraft['type'],
                    'lat_current': user.coordinates[0],
                    'lon_current': user.coordinates[1],
                    'pos_current': user.coordinates
                })
        if not flattened_current_users:
            return self.bundled_events
        
        # Process into dataframes
        current_users = pd.DataFrame(flattened_current_users)
        acids = current_users['accountID'].to_list()

        db = self.mongo_db_database
        user_coll = db['users']
        configs = self.config.load_config()

        # Handle users going offline
        going_offline = list(user_coll.find({
            'Online': True,
            'accountID': {'$nin': acids}
        }))
        for doc in going_offline:
            if datetime.now() - doc['lastOnline'] > timedelta(minutes=1):
                self.logger.debug(f"Account ID: {doc['accountID']} is offline.")
                evt = {'eventType':'offline', 'timestamp':doc['lastOnline']}
                self.batch_processors['users'].add_to_batch(
                    UpdateOne(
                        {'accountID': doc['accountID']},
                        {'$set':{'Online':False, 'lastOnline':datetime.now()}, '$push':{'events':evt}}
                    )
                )
                self.bundled_events.append({'type': 'activity_change','data': {'acid':doc['accountID'],'status': "offline"}})
        # handle users going online
        going_online = list(user_coll.find({
            'Online': False,
            'accountID': {'$in': acids}
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

        existing_docs = list(user_coll.find({'accountID': {'$in': acids}}))
        if existing_docs:
            df_existing = pd.DataFrame(existing_docs)
            if 'lastPosition' in df_existing.columns:
                positions = df_existing['lastPosition'].apply(
                    lambda x: x if isinstance(x, list) and len(x) >= 2 else [np.nan, np.nan]
                )
                df_existing[['lat_old', 'lon_old']] = pd.DataFrame(positions.tolist(), index=df_existing.index)
            else:
                df_existing['lat_old'] = np.nan
                df_existing['lon_old'] = np.nan
        else:
            df_existing = pd.DataFrame(columns=['accountID', 'currentCallsign', 'currentAircraft', 'lat_old', 'lon_old'])

        # merge and calculate distance
        df_merged = pd.merge(current_users, df_existing, on='accountID', how='left')
        df_merged['distance'] = self.calculate_aircraft_change(
            df_merged['lat_old'].astype(float),
            df_merged['lon_old'].astype(float),
            df_merged['lat_current'].astype(float),
            df_merged['lon_current'].astype(float)
        )

        # create event masks
        current_datetime = datetime.now()
        str_current_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        
        df_merged['is_new'] = df_merged['currentCallsign'].isna()
        df_merged['is_ac_change'] = df_merged['currentAircraft'].notna() & (df_merged['aircraft_current'] != df_merged['currentAircraft'])
        df_merged['is_cs_change'] = df_merged['currentCallsign'].notna() & (df_merged['callsign_current'] != df_merged['currentCallsign'])
        df_merged['is_teleport'] = df_merged['distance'].notna() & (df_merged['distance'] >= 50)




        for row in df_merged.itertuples():
            acid = row.accountID
            callsign = row.callsign_current
            aircraft = row.aircraft_current
            position = row.pos_current
            
            evts = []

            if row.is_new and configs['displayNewAccounts']:
                self.logger.debug(f"New account detected: {acid} with callsign {callsign}.")
                self.bundled_events.append({'type': 'new_account', 'data': {'acid': acid, 'callsign': callsign}})

            if getattr(row, 'is_teleport', False):
                self.logger.debug(f"Account ID: {acid} teleported {round(row.distance)} km.")
                evts.append({
                    'eventType': 'teleportation',
                    'oldLatitude': row.lat_old,
                    'oldLongitude': row.lon_old,
                    'newLatitude': row.lat_current,
                    'newLongitude': row.lon_current,
                    'timestamp': current_datetime,
                    'distance': row.distance
                })
                if configs["displayTeleporations"]:
                    self.bundled_events.append({
                        'type': 'teleportation', 
                        'data': {
                            'acid': acid, 
                            'oldLatitude': row.lat_old,
                            'oldLongitude': row.lon_old,
                            'newLatitude': row.lat_current,
                            'newLongitude': row.lon_current,
                            'timestamp': str_current_datetime,
                            'distance': row.distance
                        }
                    })

            if getattr(row, 'is_ac_change', False):
                self.logger.debug(f"Aircraft change: {acid} from {row.currentAircraft} to {aircraft}")
                evts.append({
                    'eventType': 'aircraftChange',
                    'oldAircraft': row.currentAircraft,
                    'newAircraft': aircraft,
                    'timestamp': current_datetime
                })
                if configs['displayAircraftChanges']:
                    self.bundled_events.append({
                        'type': 'aircraft_change', 
                        'data': {'acid': acid, 'oldAircraft': row.currentAircraft, 'newAircraft': aircraft}
                    })

            if getattr(row, 'is_cs_change', False):
                self.logger.debug(f"Callsign change: {acid} from {row.currentCallsign} to {callsign}")
                evts.append({
                    'eventType': 'callsignChange',
                    'oldCallsign': row.currentCallsign,
                    'newCallsign': callsign,
                    'timestamp': current_datetime
                })
                if configs['displayCallsignChanges']:
                    self.bundled_events.append({
                        'type': 'callsign_change', 
                        'data': {'acid': acid, 'oldCallsign': row.currentCallsign, 'newCallsign': callsign}
                    })

            # Upsert user document
            upsert = UpdateOne(
                {'accountID': acid},
                {
                    '$setOnInsert': {'accountID': acid},
                    '$set': {
                        'currentCallsign': callsign,
                        'currentAircraft': aircraft,
                        'Online': True,
                        'lastOnline': current_datetime,
                        'lastPosition': position
                    },
                    '$addToSet': {'pastCallsigns': callsign},
                    **({'$push': {'events': {'$each': evts}}} if evts else {})
                },
                upsert=True
            )
            self.batch_processors['users'].add_to_batch(upsert)

        self.batch_processors['users'].flush_batch()
        return self.bundled_events