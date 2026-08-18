import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv
import os
import logging
import json
import asyncio
from pymongo import MongoClient

from cogs import geofs_monitor,config,chat_logging

load_dotenv()
BOT_TOKEN = os.getenv('DISCORD_TOKEN')
SESSION_ID = os.getenv('SESSION_ID')
ACCOUNT_ID = os.getenv('ACCOUNT_ID')
DATABASE_TOKEN = os.getenv('DATABASE_TOKEN')
DATABASE_NAME = os.getenv('DATABASE_NAME')
DATABASE_IP = os.getenv('DATABASE_IP')
DATABASE_USER = os.getenv('DATABASE_USER')

class Orion(commands.Bot):
    def __init__(self):
        # Logger setup
        self.logger = logging.getLogger('Orion-Logger')
        self.logger.setLevel(logging.DEBUG)
        self.stream_handler = logging.StreamHandler()
        self.logger.addHandler(self.stream_handler)

        intents = discord.Intents.all()
        super().__init__(command_prefix="!", intents=intents)

        self.lock = asyncio.Lock()

        mongo_db_url = f"mongodb://{DATABASE_USER}:{DATABASE_TOKEN}@{DATABASE_IP}:27017/?directConnection=true&serverSelectionTimeoutMS=2000&authSource={DATABASE_NAME}"
        self.mongo_db_database = MongoClient(mongo_db_url)[DATABASE_NAME]

    async def on_ready(self):
        self.logger.info(f'{self.user} has connected to Discord')

    async def setup_hook(self):
        self.logger.info("Starting up...")
        self.logger.info("Loading extensions...")
        await self._load_extensions()
        self.config = self.get_cog("Config").load_config()
        self.logger.info("Syncing commands...")
        try:
            synced = await self.tree.sync()
            self.logger.info(f"Synced {len(synced)} command(s)")
        except Exception as e:
            self.logger.error(f"Exception while syncing commands. Error: {e}")

        self.logger.log(20, "Starting task processing loops...")

        self.logger.info("Connecting to discord...")

        self.process_tasks.start()

    async def _load_extensions(self) -> None:
        await self.add_cog(chat_logging.ChatLogging(self, SESSION_ID, ACCOUNT_ID, self.mongo_db_database))
        await self.add_cog(config.Config(self))
        await self.add_cog(geofs_monitor.GeoFSMonitor(self, self.mongo_db_database, self.logger))

    @tasks.loop(seconds=1)
    async def process_tasks(self):
        geofs_monitor = self.get_cog("GeoFSMonitor")
        # Collect data
        data = geofs_monitor.process_users()

        # process tasks from the queue
        for embed_item in data:
            if embed_item['type'] == 'aircraft_change':
                await self.process_aircraft_change(embed_item['data'])
            elif embed_item['type'] == 'new_account':
                await self.process_new_account(embed_item['data'])
            elif embed_item['type'] == 'callsign_change':
                await self.process_callsign_change(embed_item['data'])
            elif embed_item['type'] == 'teleportation':
                await self.process_teleportation(embed_item['data'])
            elif embed_item['type'] == 'activity_change':
                await self.process_activity_change(embed_item['data'])
            else:
                self.logger.error(f"Error processing task {embed_item['type']}")
                continue
    
    async def process_aircraft_change(self, data):
        channel = await self.get_channel_config("aircraft-change")
        if not channel or not self.config.get("displayAircraftChanges", True):
            return
        
        embeds = [
            discord.Embed(
                title="Aircraft Change",
                description=f"Callsign: {change_data['callsign']}\n Old Aircraft: {change_data['oldAircraft']}\n New Aircraft: {change_data['newAircraft']}",
                color=discord.Color.green()
            ) for change_data in data
        ]
        await self.send_embeds(channel, embeds)

    async def process_new_account(self, data):
        channel = await self.get_channel_config("new-account")
        if not channel or not self.config.get("displayNewAccounts", True):
            return
        
        embeds = [
            discord.Embed(
                title="New Account",
                description=f"Account ID: {account_data['acid']}\n Callsign: {account_data['callsign']}",
                color=discord.Color.green()
            ) for account_data in data
        ]
        await self.send_embeds(channel, embeds)

    async def process_callsign_change(self, data):
        channel = await self.get_channel_config("callsign-change")
        if not channel or not self.config.get("displayCallsignChanges", True):
            return
        
        embeds = [
            discord.Embed(
                title="Callsign Change",
                description=f"Acoount ID: {callsign_data['acid']}\n Old Callsign: {callsign_data['oldCallsign']}\n New Callsign: {callsign_data['newCallsign']}",
                color=discord.Color.green()
            ) for callsign_data in data
        ]
        await self.send_embeds(channel, embeds)

    async def process_teleportation(self, data):
        channel = await self.get_channel_config("teleporation")
        if not channel or not self.config.get("displayTeleporations", True):
            return
        
        embeds = [
            discord.Embed(
                title="Teleporation",
                description=f"{teleporation_data['acid']}\n Old Position: {teleporation_data['oldLatitude']}, {teleporation_data['oldLongitude']}\n New Position: {teleporation_data['newLatitude']}, {teleporation_data['newLongitude']}\n Distance: {teleporation_data['distance']} km",
                color=discord.Color.green()
            ) for teleporation_data in data
        ]
        await self.send_embeds(channel, embeds)

    async def process_activity_change(self, data):
        channel = await self.get_channel_config("activity-change")
        if not channel or not self.config.get("displayActivityChanges", True):
            return
        
        embed = discord.Embed(
            title="Activity Change",
            description=f"{data['acid']}\n Status: {data['status']}",
            color=discord.Color.green()
        )
        await self.send_embeds(channel, embed)
    
    async def send_embeds(self, channel, embeds):
        async with self.lock:
            for embed in embeds:
                await channel.send(embed=embed)
                await asyncio.sleep(self.throttleInterval)

    async def get_channel_config(self, event_type): # gets the channel for the event type
        self.config
        if event_type == "aircraft-change":
            channel_id = self.config["aircraftChangeLogChannel"]
        elif event_type == "new-account":
            channel_id = self.config["newAccountLogChannel"]
        elif event_type == "callsign-change":
            channel_id = self.config["callsignChangeLogChannel"]
        elif event_type == "teleporation":
            channel_id = self.config["teleporationLogChannel"]
        elif event_type == "activity-change":
            channel_id = self.config["activityChangeLogChannel"]
        else:
            self.logger.log(40, f"Invalid event type: {event_type}")
            return None

        channel = self.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.fetch_channel(channel_id)
            except discord.NotFound:
                self.logger.warning(f"Channel ID for '{event_type}' was not found.")
                return None
            except discord.HTTPException as e:
                self.logger.warning(f"Failed to fetch channel for '{event_type}': {e}")
                return None

        return channel

bot = Orion()

@bot.event
async def on_guild_join(guild):
    async for entry in guild.audit_logs(action=discord.AuditLogAction.bot_add):
        bot.logger.info(f"Joined {guild.name}")

@bot.tree.command(name="ping", description="Check bot connection and latency.")
async def ping(interaction: discord.Interaction):
    delay = round(bot.latency * 1000)
    embed = discord.Embed(title="Pong!", description=f"Latency: {delay}ms", color=discord.Color.green())
    await interaction.response.send_message(embed=embed)

def main():
    bot.run(BOT_TOKEN)

if __name__ == "__main__":
    main()