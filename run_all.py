# run_all.py
import asyncio

from main_bot import main as main_bot_main
from login_bot import main as login_bot_main
from worker_forward import loop_worker as worker_main
from profile_enforcer import main as enforcer_main


async def main():
    await asyncio.gather(
        login_bot_main(),     # @SpinifyLoginBot
        main_bot_main(),      # @SpinifyAdsBot
        worker_main(),        # forwarder
        enforcer_main(),      # profile fixer
    )

if __name__ == "__main__":
    asyncio.run(main())
