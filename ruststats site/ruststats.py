import json, aiohttp, asyncio, validators

CONFIG = ''
with open("config.json", "r") as config_file:
    CONFIG = json.load(config_file)
    
async def get_ids(submittedtext):
        submittedtext = str(submittedtext)
        steamid = 0
        
        if validators.url(submittedtext):  # If it's a link, check what type
            mysplit = submittedtext.split("/")

            if mysplit[3] == "id":
                steamid = await get_id_from_steam(mysplit[4])

            if mysplit[3] == "profiles":
                steamid = mysplit[4]
                
        else:  # Make sure it's a steam ID and then move on.
            if len(submittedtext) != 17:
                return
            steamid = submittedtext
        return steamid

async def get_id_from_steam(url):
        """Takes the URL (well part of it) and returns a steam ID"""

        url = (
            f"https://api.steampowered.com/ISteamUser/ResolveVanityURL/v1/?format=json&"
            f"key={CONFIG['steam_token']}&vanityurl={url}&url_type=1"
        )
        data = ""
        async with aiohttp.ClientSession(
            headers={"Authorization": CONFIG['steam_token']}
        ) as session:

            async with session.get(url=url) as r:
                response = await r.json()
        data = response
        return data["response"]["steamid"] if data["response"]["steamid"] else 0


async def get_rust_stats(steamid):
    url = f"http://api.steampowered.com/ISteamUserStats/GetUserStatsForGame/v0002/?appid=252490&key={CONFIG['steam_token']}&steamid={steamid}"
    
    data = ''
    async with aiohttp.ClientSession(headers={"Authorization": CONFIG['steam_token']}) as session:
        async with session.get(url=url) as r:
            response = await r.json()
    data = response
    return data


async def stats(submittedtext):
    steamid = await get_ids(submittedtext)
    results = ''
    if not steamid:
        results = {"error": "Not a valid steamid or steam url"}
        return results

    results = await get_rust_stats(steamid)
    return results

mydata = asyncio.run(stats(76561199012582724))
print(json.dumps(mydata, indent=4))