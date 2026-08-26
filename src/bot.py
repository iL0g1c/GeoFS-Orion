import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv
import os
import logging
import time
import asyncio
from pymongo import MongoClient
import sys

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
        self.logger.setLevel(logging.INFO)
        self.stream_handler = logging.StreamHandler(sys.stdout)
        self.logger.addHandler(self.stream_handler)

        intents = discord.Intents.all()
        super().__init__(command_prefix="!", intents=intents)

        self.lock = asyncio.Lock()
        self.throttleInterval = 0.2

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

        self.logger.info("Starting task processing loops...")

        self.logger.info("Connecting to discord...")

        self.process_tasks.start()

    async def _load_extensions(self) -> None:
        await self.add_cog(chat_logging.ChatLogging(self, SESSION_ID, ACCOUNT_ID, self.mongo_db_database, self.logger))
        await self.add_cog(config.Config(self))
        await self.add_cog(geofs_monitor.GeoFSMonitor(self, self.mongo_db_database, self.logger))

    @tasks.loop(seconds=1)
    async def process_tasks(self):
        try:
            start_time = time.time()
            geofs_monitor = self.get_cog("GeoFSMonitor")
            # Collect data
            data = await asyncio.to_thread(geofs_monitor.process_users)

            # process tasks from the queue
            batches = {
                "aircraft_change": [],
                "new_account": [],
                "callsign_change": [],
                "teleportation": [],
                "activity_change": [],
            }

            for item in data:
                event_type = item.get("type")
                if event_type in batches:
                    batches[event_type].append(item.get("data", item))

            await self.dispatch_messages_batch("aircraft_change", batches["aircraft_change"], self.format_aircraft_messages)
            await self.dispatch_messages_batch("new_account", batches["new_account"], self.format_new_account_messages)
            await self.dispatch_messages_batch("callsign_change", batches["callsign_change"], self.format_callsign_messages)
            await self.dispatch_messages_batch("teleportation", batches["teleportation"], self.format_teleport_messages)
            await self.dispatch_messages_batch("activity_change", batches["activity_change"], self.format_activity_messages)

            end_time = time.time()
            self.logger.info(f"The loop took {end_time - start_time:.2f} seconds to execute.")
        except Exception as e:
            self.logger.error(f"process_tasks loop failed this tick: {e}")

    @process_tasks.before_loop
    async def before_process_tasks(self):
        await self.wait_until_ready()

    @process_tasks.error
    async def process_tasks_error(self, error):
        self.logger.error(f"Process_tasks loop crashed with error: {error}")

    async def dispatch_messages_batch(self, event_type: str, items: list[dict], formatter_fn):
        if not items:
            return
            
        channel = await self.get_channel_config(event_type)
        if not channel:
            return

        message_content = formatter_fn(items)
        if message_content:
            async with self.lock:
                try:
                    await channel.send(message_content)
                    await asyncio.sleep(self.throttleInterval)
                except discord.HTTPException as e:
                    self.logger.warning(f"Discord API failed while sending {event_type} messages: {e}")
                except Exception as e:
                    self.logger.error(f"Unexpected error sending {event_type} messages: {e}")

    def format_aircraft_messages(self, items: list[dict]) -> str:
        # \u001b[34m = Blue, \u001b[32m = Green, \u001b[33m = Yellow, \u001b[36m = Cyan, \u001b[0m = Reset
        lines = ["```ansi"]
        for item in items[:15]:
            lines.append(
                f"\u001b[34m[AIRCRAFT]\u001b[0m \u001b[32m{item['acid']}\u001b[0m: "
                f"\u001b[33m{item['oldAircraft']}\u001b[0m -> \u001b[36m{item['newAircraft']}\u001b[0m"
            )
        if len(items) > 15:
            lines.append(f"\u001b[30m... and {len(items) - 15} more\u001b[0m")
        lines.append("```")
        return "\n".join(lines)

    def format_teleport_messages(self, items: list[dict]) -> str:
        # \u001b[35m = Magenta, \u001b[31m = Red
        lines = ["```ansi"]
        for item in items[:15]:
            lines.append(
                f"\u001b[35m[TELEPORT]\u001b[0m Account \u001b[32m{item['acid']}\u001b[0m moved \u001b[31m{round(item['distance'])} km\u001b[0m"
            )
        if len(items) > 15:
            lines.append(f"\u001b[30m... and {len(items) - 15} more\u001b[0m")
        lines.append("```")
        return "\n".join(lines)

    def format_callsign_messages(self, items: list[dict]) -> str:
        lines = ["```ansi"]
        for item in items[:15]:
            lines.append(
                f"\u001b[36m[CALLSIGN]\u001b[0m Account \u001b[32m{item['acid']}\u001b[0m: "
                f"\u001b[33m{item['oldCallsign']}\u001b[0m -> \u001b[36m{item['newCallsign']}\u001b[0m"
            )
        if len(items) > 15:
            lines.append(f"\u001b[30m... and {len(items) - 15} more\u001b[0m")
        lines.append("```")
        return "\n".join(lines)

    def format_new_account_messages(self, items: list[dict]) -> str:
        lines = ["```ansi"]
        for item in items[:15]:
            lines.append(
                f"\u001b[32m[NEW ACCT]\u001b[0m \u001b[32m{item['acid']}\u001b[0m (\u001b[36m{item['callsign']}\u001b[0m)"
            )
        if len(items) > 15:
            lines.append(f"\u001b[30m... and {len(items) - 15} more\u001b[0m")
        lines.append("```")
        return "\n".join(lines)

    def format_activity_messages(self, items: list[dict]) -> str:
        lines = ["```ansi"]
        for item in items[:15]:
            status_color = "\u001b[32m" if item['status'] == 'online' else "\u001b[31m"
            lines.append(
                f"\u001b[33m[ACTIVITY]\u001b[0m Account \u001b[36m{item['acid']}\u001b[0m is now {status_color}{item['status']}\u001b[0m"
            )
        if len(items) > 15:
            lines.append(f"\u001b[30m... and {len(items) - 15} more\u001b[0m")
        lines.append("```")
        return "\n".join(lines)

    async def get_channel_config(self, event_type): # gets the channel for the event type
        self.config
        if event_type == "aircraft_change":
            channel_id = self.config["aircraftChangeLogChannel"]
        elif event_type == "new_account":
            channel_id = self.config["newAccountLogChannel"]
        elif event_type == "callsign_change":
            channel_id = self.config["callsignChangeLogChannel"]
        elif event_type == "teleportation":
            channel_id = self.config["teleporationLogChannel"]
        elif event_type == "activity_change":
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