import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv
import os
import logging
import json

load_dotenv()
BOT_TOKEN = os.getenv('DISCORD_TOKEN')

class Orion(commands.Bot):
    def __init__(self):
        # Logger setup
        self.logger = logging.getLogger('Orion-Logger')
        self.logger.setLevel(logging.DEBUG)
        self.stream_handler = logging.StreamHandler()
        self.logger.addHandler(self.stream_handler)

        intents = discord.Intents.all()
        super().__init__(command_prefix="!", intents=intents)

    def load_config(self, config_path):
        with open(config_path, "r") as f:
            return json.load(f)

    async def on_ready(self):
        self.logger.info(f'{self.user} has connected to Discord')

    async def setup_hook(self):
        self.logger.info("Starting up...")
        self.logger.info("Loading extensions...")
        await self._load_extensions()
        self.logger.info("Syncing commands...")
        try:
            synced = await self.tree.sync()
            self.logger.info(f"Synced {len(synced)} command(s)")
        except Exception as e:
            self.logger.error(f"Exception while syncing commands. Error: {e}")

        self.logger.info("Connecting to discord...")

    async def _load_extensions(self) -> None:
        for extension in ("chat_logging",   ):
            await self.load_extension(f"cogs.{extension}")

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