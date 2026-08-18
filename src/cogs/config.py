from discord.ext import commands
import json

class Config(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        pass
    def load_config(self):
        with open("src/config.json", "r") as f:
            return json.load(f)

async def setup(bot: commands.Bot):
    await bot.add_cog(Config(bot))