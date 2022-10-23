import asyncio
import datetime
import json
from re import I
import aiohttp
import validators

from datetime import datetime
import datetime
from datetime import timezone
from unicodedata import name
from fuzzywuzzy import fuzz
import aiosqlite
import glob
import requests

# noinspection SpellCheckingInspection


class BMAPI:
    def __init__(self):

        with open("./json/config.json", "r") as config_file:
            mytokens = json.load(config_file)
        self.config = mytokens
        self.url_base = "https://api.battlemetrics.com/"
        self.bmtoken = f"Bearer {mytokens['battlemetrics_token']}"
        self.vpntoken = mytokens["vpn_token"]
        self.steamtoken = mytokens["steam_token"]
        self.rustbannedapi = mytokens["rustbanned_token"]

    async def getbanlist(self, orgid, pagesize):
        bmtoken = f'Bearer {self.config["battlemetrics_token"]}'
        url = f"https://api.battlemetrics.com/bans?filter[organization]={orgid}&include=user,server&page[size]={pagesize}"
        response = ""
        async with aiohttp.ClientSession(headers={"Authorization": bmtoken}) as session:
            async with session.get(url=url) as r:
                response = await r.json()
        data = response
        return data

    async def GetServerInfo(self, serverid):
        db = DataBase()
        serverinfo = await db.get_serverinfo(serverid)
        if len(serverinfo) > 0:
            return serverinfo
        url = f"https://api.battlemetrics.com/servers/{serverid}?include=organization"
        async with aiohttp.ClientSession(headers={"Authorization": self.bmtoken}) as session:
            async with session.get(url=url) as r:
                response = await r.json()
        data = response
        serverinfo = {
            'serverid': data['data']['id'],
            'servername': data['data']['attributes']['name'],
            'orgid': data['included'][0]['id'],
            'orgname': data['included'][0]['attributes']['name']
        }
        await db.add_serverinfo(serverinfo)
        return serverinfo

    async def getbanlist_server(self, serverid, pagesize):
        bmtoken = f'Bearer {self.config["battlemetrics_token"]}'
        url = f"https://api.battlemetrics.com/bans?filter[server]={serverid}&page[size]={pagesize}"
        response = ""
        async with aiohttp.ClientSession(headers={"Authorization": bmtoken}) as session:
            async with session.get(url=url) as r:
                response = await r.json()
        data = response
        return data

    async def get_names(self, bmid):
        db = DataBase()
        weekago = datetime.datetime.now(
            timezone.utc) - datetime.timedelta(hours=72)
        weekago = str(weekago)
        weekago = weekago.split(" ")
        weekago = weekago[0]
        weekago = datetime.datetime.strptime(weekago, '%Y-%m-%d')
        fromdb = await db.get_playerinfo(bmid=bmid)
        lastupdate = ''
        if fromdb:
            lastupdate = fromdb['updatedat']
            lastupdate = lastupdate.split(" ")
            lastupdate = lastupdate[0]
            lastupdate = datetime.datetime.strptime(
                lastupdate, '%Y-%m-%d')
            if lastupdate > weekago:
                names = fromdb['names']
                names = names.split(",")
                return names

        response = ""
        my_headers = {
            "Authorization": f"{self.config['battlemetrics_token']}",
            "Content-Type": "application/json",
        }
        url = f"https://api.battlemetrics.com/players/{bmid}"
        async with aiohttp.ClientSession(headers=my_headers) as session:
            async with session.get(url=url, params={"include": "identifier"}) as r:
                response = await r.json()
        data = response
        response = data

        names = []
        if response.get('included'):
            usefuldata = response["included"]
            for identifier in usefuldata:
                if identifier["attributes"]["type"] == "name":
                    names.append(identifier["attributes"]["identifier"])
        return names

    async def compare(self, person1, person2):
        samenames = []
        for name1 in person1:
            for name2 in person2:
                match_ratio = fuzz.ratio(name1, name2)
                samenames.append(
                    {"match_ratio": match_ratio, "name1": name1, "name2": name2}
                )
        sorted_name_matches = sorted(
            samenames, key=lambda k: k["match_ratio"], reverse=True
        )
        return sorted_name_matches[:5]

    async def get_ids(self, submittedtext: str):
        db = DataBase()
        userinfo = {"bmid": 0, "steamid": 0}
        bmid = 0
        steamid = 0
        # Convert the submitted URL or ID into a Battlemetrics ID.
        if validators.url(submittedtext):  # If it's a link, check what type
            mysplit = submittedtext.split("/")

            if mysplit[3] == "id":
                steamid = await self.get_id_from_steam(mysplit[4])

            if mysplit[3] == "profiles":
                steamid = mysplit[4]

            if mysplit[3] == "rcon":
                bmid = mysplit[5]
        else:  # Make sure it's a steam ID and then move on.
            if len(submittedtext) != 17:
                return userinfo
            steamid = submittedtext

        if not steamid and not bmid:
            return userinfo

        if steamid:
            playerinfo = await db.get_playerinfo(steamid=steamid)
            if playerinfo:
                bmid = playerinfo['bmid']
            else:
                bmid = await self.search_bm(steamid)
        if bmid:
            playerinfo = await db.get_playerinfo(bmid=bmid)
            if playerinfo:
                steamid = playerinfo['steamid']
            userinfo = {"steamid": steamid, "bmid": bmid}

        return userinfo

    async def stats(self, bmid):
        kda_results_day = await self.kda_day(bmid)
        kda_results_week = await self.kda_week(bmid)
        stats = {"kills_day": 0, "deaths_day": 0,
                 "kills_week": 0, "deaths_week": 0}

        for i in kda_results_day["data"]:
            if (
                i.get("attributes")
                and i["attributes"].get("data")
                and i["attributes"]["data"].get("killer_id")
            ):
                if i["attributes"]["data"]["killer_id"] == int(bmid):
                    stats["kills_day"] = stats["kills_day"] + 1
                else:
                    stats["deaths_day"] = stats["deaths_day"] + 1

        for i in kda_results_week["data"]:
            if (
                i.get("attributes")
                and i["attributes"].get("data")
                and i["attributes"]["data"].get("killer_id")
            ):
                if i["attributes"]["data"]["killer_id"] == int(bmid):
                    stats["kills_week"] = stats["kills_week"] + 1
                else:
                    stats["deaths_week"] = stats["deaths_week"] + 1

        while kda_results_day["links"].get("next"):
            myextension = kda_results_day["links"]["next"]
            kda_results_day = await self.additional_data(myextension)
            await asyncio.sleep(0.2)
            for i in kda_results_day["data"]:
                if (
                    i.get("attributes")
                    and i["attributes"].get("data")
                    and i["attributes"]["data"].get("killer_id")
                ):
                    if i["attributes"]["data"]["killer_id"] == int(bmid):
                        stats["kills_day"] = stats["kills_day"] + 1
                    else:
                        stats["deaths_day"] = stats["deaths_day"] + 1

        while kda_results_week["links"].get("next"):
            myextension = kda_results_week["links"]["next"]
            kda_results_week = await self.additional_data(myextension)
            await asyncio.sleep(0.2)
            for i in kda_results_week["data"]:
                if (
                    i.get("attributes")
                    and i["attributes"].get("data")
                    and i["attributes"]["data"].get("killer_id")
                ):
                    if i["attributes"]["data"]["killer_id"] == int(bmid):
                        stats["kills_week"] = stats["kills_week"] + 1
                    else:
                        stats["deaths_week"] = stats["deaths_week"] + 1
        return stats

    async def playernotes(self, bmid, action):
        db = DataBase()
        action = action.lower()
        notes = {}
        notes = await db.get_notes(bmid=bmid)
        if action == 'normal' and notes:
            return notes

        url_extension = f"players/{bmid}/relationships/notes?include=user,organization&page[size]=100"
        url = f"{self.url_base}{url_extension}"
        my_headers = {"Authorization": self.bmtoken}
        response = ""
        async with aiohttp.ClientSession(headers=my_headers) as session:
            async with session.get(url=url) as r:
                response = await r.json()
        data = response
        notemaker_name, organization_name, notes, notemaker_id, organization_id = (
            "Autogenerated",
            0,
            {},
            0,
            0,
        )
        for a in data["data"]:
            organization_id = a["relationships"]["organization"]["data"]["id"]
            if a["relationships"].get("user"):
                notemaker_id = a["relationships"]["user"]["data"]["id"]
            noteid = a["id"]
            note = a["attributes"]["note"]
            for b in data["included"]:
                if notemaker_id:
                    if b["type"] == "user":
                        if b["id"] == notemaker_id:
                            notemaker_name = b["attributes"]["nickname"]
                if b["type"] == "organization":
                    if b["id"] == organization_id:
                        organization_name = b["attributes"]["name"]
            notes[noteid] = {
                "noteid": noteid,
                "organization_id": organization_id,
                "notemaker_id": notemaker_id,
                "organization_name": organization_name,
                "notemaker_name": notemaker_name,
                "note": note,
                "bmid": bmid
            }
        if action == 'update':
            await db.add_notes(notes)
        return notes

    async def relatedplayers(self, bmid):
        db = DataBase()

        """Grabs the information of all related players."""
        url_extension = (
            f"players/{bmid}/relationships/related-identifiers?version=^0.1.0"
            f"&filter[matchIdentifiers]=ip"
            f"&filter[identifiers]=ip&include=player,identifier&page[size]=100"
        )
        url = f"{self.url_base}{url_extension}"

        my_headers = {"Authorization": self.bmtoken}

        response = ""
        async with aiohttp.ClientSession(headers=my_headers) as session:
            async with session.get(url=url) as r:
                try:
                    response = await r.json()
                except:
                    altinfo = {}
                    timeout = (
                        "I was unable to gather the related players information as the request timed out.\n "
                        "Please refer to their battlemetrics page for additional information."
                    )
                    altinfo["proxycount"] = 0
                    altinfo["relatedcount"] = 0
                    altinfo["timeout"] = timeout
                    return altinfo

                if r.status != 200:

                    altinfo = {}
                    timeout = (
                        "I was unable to gather the related players information as the request timed out.\n "
                        "Please refer to their battlemetrics page for additional information."
                    )
                    altinfo["proxycount"] = 0
                    altinfo["relatedcount"] = 0
                    altinfo["timeout"] = timeout
                    return altinfo

        data = response

        related, processed, altinfo, relatedcount, proxycount = [], [], {}, 0, 0

        if data["data"]:
            for d in data["data"]:
                if d.get("attributes"):
                    if d["attributes"].get("type"):
                        if d["attributes"]["type"] == "ip":
                            if d["attributes"].get("identifier"):
                                userip = d["attributes"]["identifier"]
                                searchedip = await self.search_ip(userip)
                                if (
                                    searchedip["security"]["vpn"]
                                    or searchedip["security"]["proxy"]
                                    or searchedip["security"]["tor"]
                                    or searchedip["security"]["relay"]
                                ):
                                    if (
                                        d["type"] == "relatedIdentifier"
                                        and d.get("attributes")
                                        and d["attributes"]["type"] == "ip"
                                    ):
                                        if d["attributes"]["metadata"].get(
                                            "connectionInfo"
                                        ):
                                            for rp in d["relationships"][
                                                "relatedPlayers"
                                            ]["data"]:
                                                proxycount += 1
                                    continue
                            else:
                                if (
                                    d["attributes"]["metadata"]["connectionInfo"]["tor"]
                                    or d["attributes"]["metadata"]["connectionInfo"][
                                        "datacenter"
                                    ]
                                    or d["attributes"]["metadata"]["connectionInfo"][
                                        "proxy"
                                    ]
                                ):
                                    for rp in d["relationships"]["relatedPlayers"][
                                        "data"
                                    ]:
                                        proxycount += 1
                                    continue
                if d["type"] == "relatedIdentifier":
                    if (
                        d["type"] == "relatedIdentifier"
                        and d.get("attributes")
                        and d["attributes"]["type"] == "ip"
                    ):
                        if d["attributes"]["metadata"].get("connectionInfo"):
                            for rp in d["relationships"]["relatedPlayers"]["data"]:
                                if not rp["id"] in related and not rp["id"] == bmid:
                                    related.append(rp["id"])

            relatedcount = len(related)

            count = 0
            if relatedcount:
                for d in data["included"]:
                    if d["type"] == "identifier":
                        altbmid = d["relationships"]["player"]["data"]["id"]
                        if altbmid in related and altbmid not in processed:
                            processed.append(altbmid)
                            # Possible Bug: Seems like noone cares about this variable
                            # noinspection PyUnusedLocal
                            # proxy = d["attributes"]["metadata"]["connectionInfo"]["proxy"]

                            altinfo[altbmid] = {
                                "proxy": "No",
                                "bmid": altbmid,
                                "playerinfo": "pi",
                                "notes": "Notes",
                                "serverbans": "serverbans",
                                "gamebans": "gamebans",
                                "country": d["attributes"]["metadata"]["country"],
                                "lastcheck": d["attributes"]["metadata"]["lastCheck"],
                                "isp": d["attributes"]["metadata"]["connectionInfo"][
                                    "isp"
                                ],
                            }
                            count += 1

        altinfo["proxycount"] = proxycount
        altinfo["relatedcount"] = relatedcount
        altinfo["timeout"] = ""
        return altinfo

    async def relatedplayers2(self, bmid, action=None):
        db = DataBase()
        if action:
            action = action.lower()
        # Get the times sorted.
        weekago = datetime.datetime.now(
            timezone.utc) - datetime.timedelta(hours=168)
        weekago = str(weekago)
        weekago = weekago.split(" ")
        weekago = weekago[0]
        weekago = datetime.datetime.strptime(weekago, '%Y-%m-%d')
        fromdb = await db.get_playerinfo(bmid=bmid)
        lastupdate = ''
        if fromdb:
            lastupdate = fromdb['updatedat']
            lastupdate = lastupdate.split(" ")
            lastupdate = lastupdate[0]
            lastupdate = datetime.datetime.strptime(
                lastupdate, '%Y-%m-%d')
            if lastupdate < weekago:
                action = 'force'

        if not action == 'force':
            relatedplayers = {}
            if len(fromdb['relatedplayers']):
                fromdb = fromdb['relatedplayers'].split(',')
                for i in fromdb:
                    relatedplayers[i] = {'bmid': i}
            ips = await db.get_ips()
            for i in ips:
                connectedto = ips[i]['connectedto'].split(',')
                for j in connectedto:
                    if j in relatedplayers:
                        relatedplayers[j]['ipinfo'] = ips[i]
            return relatedplayers

        token = self.config['battlemetrics_token']
        url = f"https://api.battlemetrics.com/players/{bmid}/relationships/related-identifiers?version=^0.1.0&page[size]=100&access_token={token}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url=url) as r:
                try:
                    response = await r.json()
                except:
                    altinfo = {}
                    timeout = (
                        "I was unable to gather the related players information as the request timed out.\n "
                        "Please refer to their battlemetrics page for additional information."
                    )
                    altinfo["proxycount"] = 0
                    altinfo["relatedcount"] = 0
                    altinfo["timeout"] = timeout
                    return altinfo

                if r.status != 200:

                    altinfo = {}
                    timeout = (
                        "I was unable to gather the related players information as the request timed out.\n "
                        "Please refer to their battlemetrics page for additional information."
                    )
                    altinfo["proxycount"] = 0
                    altinfo["relatedcount"] = 0
                    altinfo["timeout"] = timeout
                    return altinfo

        proxycount = 0
        relatedcount = 0
        ipinfo = {}
        rps = []
        myrps = {}
        newip = False
        updateip = False
        connectedto = []
        for i in response['data']:
            if i['attributes']['type'] == 'ip':
                ipinfo_db = await db.get_ipinfo(ip=i['attributes']['identifier'])
                if ipinfo_db:
                    ipinfo[i['id']] = ipinfo_db
                    connectedto = ipinfo_db['connectedto'].split(',')

                    if ipinfo_db['is_vpn']:
                        proxycount += 1
                else:
                    newip = True
                    ipinfo[i['id']] = {
                        'ip': i['attributes']['identifier'],
                        'isp_id': i['attributes']['metadata']['connectionInfo']['asn'],
                        'country': i['attributes']['metadata']['country'],
                        'is_vpn': False,
                        'isp_name': i['attributes']['metadata']['connectionInfo']['isp']
                    }
                    if i['attributes']['metadata']['connectionInfo']['tor'] or i['attributes']['metadata']['connectionInfo']['datacenter'] or i['attributes']['metadata']['connectionInfo']['proxy']:
                        ipinfo[i['id']]['is_vpn'] = True
                        proxycount += 1
                    else:
                        searchedip = await self.search_ip(i['attributes']['identifier'])
                        if searchedip["security"]["vpn"] or searchedip["security"]["proxy"] or searchedip["security"]["tor"] or searchedip["security"]["relay"]:
                            ipinfo[i['id']]['is_vpn'] = True
                for r in i['relationships']['relatedPlayers']['data']:
                    if not r['id'] in connectedto:
                        connectedto.append(r['id'])
                        ipinfo_db['connected_to'] = connectedto
                        if not newip:
                            await db.update_ipinfo(ipinfo=ipinfo_db)
                    if not ipinfo[i['id']]['is_vpn']:
                        rps.append(r['id'])
                    ipinfo[i['id']]['connectedto'] = connectedto
                    myrps[r['id']] = {
                        'bmid': r['id'],
                        'ipinfo': ipinfo[i['id']]
                    }
                    relatedcount += 1
        ipinfo_db['connectedto'] = connectedto
        newdb = DataBase()
        await db.update_relatedplayers(bmid=bmid, relatedplayers=rps)
        if newip:
            await newdb.add_ipinfo(ipinfo=ipinfo)
        return myrps

    async def serverbans(self, bmid):
        #db = DataBase()
        # weekago = datetime.datetime.now(
        #    timezone.utc) - datetime.timedelta(hours=24)
        #weekago = str(weekago)
        #weekago = weekago.split(" ")
        #weekago = weekago[0]
        #weekago = datetime.datetime.strptime(weekago, '%Y-%m-%d')
        # fromdb = await db.get_playerinfo(bmid=bmid)
        #lastupdate = ''
        # if fromdb:
        #    lastupdate = fromdb['updatedat']
        #    lastupdate = lastupdate.split(" ")
        #    lastupdate = lastupdate[0]
        #    lastupdate = datetime.datetime.strptime(
        #        lastupdate, '%Y-%m-%d')
        #    if lastupdate > weekago:
        #        return fromdb['names']

        url_extension = f"bans?filter[player]={bmid}&include=user,server"
        url = f"{self.url_base}{url_extension}"
        my_headers = {"Authorization": self.bmtoken}

        response = ""
        async with aiohttp.ClientSession(headers=my_headers) as session:
            async with session.get(url=url) as r:
                response = await r.json()
        data = response
        bans, banner, bancount = {}, "", 0

        if data["meta"]["total"] > 0:
            for d in data["data"]:
                banid = d["id"]
                banreason = d["attributes"]["reason"]
                expires = (
                    d["attributes"]["expires"]
                    if d["attributes"]["expires"]
                    else "Never"
                )
                bandate = d["attributes"]["timestamp"]
                bannote = d["attributes"]["note"]
                bandate = bandate.split("T")
                bandate = bandate[0]
                servername = "Unknown"
                banner = "Unknown"
                banreason = 'Unknown'
                serverid = 0
                if d['relationships'].get('server'):
                    serverid = d["relationships"]["server"]["data"]["id"]

                    for i in data["included"]:
                        if i["type"] == "server":
                            if i["id"] == serverid:
                                servername = i["attributes"]["name"]
                    x = banreason.split("|")
                    banreason = x[0]
                    if d["relationships"].get("user"):
                        banner_id = d["relationships"]["user"]["data"]["id"]
                        for b in data["included"]:
                            if b["type"] == "user":
                                banner = (
                                    b["attributes"]["nickname"]
                                    if b["attributes"]["id"] == banner_id
                                    else "Autoban"
                                )

                bans[bancount] = {
                    "bandate": bandate,
                    "expires": expires,
                    "bannote": bannote,
                    "banid": banid,
                    "banreason": banreason,
                    "servername": servername,
                    "serverid": serverid,
                    "banner": banner,
                }
                bancount += 1
        return bans

    async def baninfo(self, banid):
        """Takes a BANID and grabs the ban information from battlemetrics

        Args:
            bmid (int): Requires a battlemetrics banid

        Returns:
            dict: reason, timestamp, note, steamid, name, profileurl, expires, bmid, server, organization, banner
        """
        url_extension = f"bans/{banid}?include=server,user,organization"
        url = f"{self.url_base}{url_extension}"

        my_headers = {"Authorization": self.bmtoken}

        response = ""
        async with aiohttp.ClientSession(headers=my_headers) as session:
            async with session.get(url=url) as r:
                response = await r.json()
        data = response

        baninfo = {
            "reason": data["data"]["attributes"]["reason"],
            "timestamp": data["data"]["attributes"]["timestamp"],
            "note": data["data"]["attributes"]["note"],
            "steamid": data["data"]["attributes"]["identifiers"][0]["identifier"],
            "name": "None Specified",
            "profileurl": "None Specified",
            "expires": 0,
            "bmid": 0,
            "server": "None Specified",
            "organization": "None Specified",
            "banner": "None Specified",
        }
        if data["data"]["attributes"]["identifiers"][0].get("metadata"):
            baninfo["name"] = data["data"]["attributes"]["identifiers"][0]["metadata"][
                "profile"
            ]["personaname"]
            baninfo["profileurl"] = data["data"]["attributes"]["identifiers"][0][
                "metadata"
            ]["profile"]["profileurl"]
        baninfo["expires"] = data["data"]["attributes"]["expires"]
        baninfo["bmid"] = data["data"]["relationships"]["player"]["data"]["id"]
        for i in data["included"]:
            if i["type"] == "server":
                baninfo["server"] = i["attributes"]["name"]
            if i["type"] == "organization":
                baninfo["organization"] = i["attributes"]["name"]
            if i["type"] == "user":
                baninfo["banner"] = i["attributes"]["nickname"]
        return baninfo

    # Grabs all the information about the user.
    async def playerinfo(self, bmid, action):
        """Takes the BMID and queries the battlemetrics API for the users player information and returns it.

        Args:
            bmid (int): Requires a battlemetrics BMID
            action (str): Set to single if you want the Kill/death info (Can delay the bot.)

        Returns:
            dict: bmid, playername, rusthours, aimtrain, steamurl, steamid, stats(kills/deaths [day/week]), updatedat, names, gamebanned, relatedplayers
        """
        weekago = datetime.datetime.now(
            timezone.utc) - datetime.timedelta(hours=168)
        weekago = str(weekago)
        weekago = weekago.split(" ")
        weekago = weekago[0]
        weekago = datetime.datetime.strptime(weekago, '%Y-%m-%d')
        db = DataBase()
        fromdb = await db.get_playerinfo(bmid=bmid)
        lastupdate = ''
        if fromdb:
            lastupdate = fromdb['updatedat']
            lastupdate = lastupdate.split(" ")
            lastupdate = lastupdate[0]
            lastupdate = datetime.datetime.strptime(
                lastupdate, '%Y-%m-%d')
            if lastupdate > weekago:
                return fromdb

        url_extension = f"players/{bmid}?include=server,identifier&fields[server]=name"
        url = f"{self.url_base}{url_extension}"
        response = ""
        async with aiohttp.ClientSession(
            headers={"Authorization": self.bmtoken}
        ) as session:
            async with session.get(url=url) as r:
                response = await r.json()
        data = response
        steamid, avatar, steamurl, rusthours, aimtrain = None, None, "", 0, 0

        if not data.get("included"):
            return steamid

        for a in data["included"]:
            if a["type"] == "identifier":
                if a.get("attributes"):
                    if a["attributes"]["type"] == "steamID":
                        steamid = a["attributes"]["identifier"]
                        if a["attributes"].get("metadata"):
                            if a["attributes"]["metadata"].get("profile"):
                                steamurl = a["attributes"]["metadata"]["profile"][
                                    "profileurl"
                                ]
                                avatar = a["attributes"]["metadata"]["profile"][
                                    "avatarfull"
                                ]
            else:
                servername = a["attributes"]["name"].lower()
                if a["relationships"]["game"]["data"]["id"] == "rust":
                    rusthours += a["meta"]["timePlayed"]
                    currplayed = a["meta"]["timePlayed"]

                    if any(
                        [
                            cond in servername
                            for cond in ["rtg", "aim", "ukn", "arena", "combattag"]
                        ]
                    ):
                        aimtrain += currplayed

        rusthours = rusthours / 3600
        rusthours = round(rusthours, 2)
        aimtrain = aimtrain / 3600
        aimtrain = round(aimtrain, 2)
        playername = data["data"]["attributes"]["name"]

        stats = "None"
        action = action.lower()
        if action == "single":
            stats = await self.stats(bmid)
        updatedat = str(datetime.datetime.now())
        names = await self.get_names(bmid)
        newnames = ''
        for i in names:
            if newnames:
                newnames += f",{i}"
            else:
                newnames = i
        names = newnames
        gamebans = await self.gamebans(steamid)
        gamebanned = False
        if gamebans:
            gamebanned = True

        playerinfo = {
            'bmid': bmid,
            "playername": playername,
            "rusthours": rusthours,
            "aimtrain": aimtrain,
            "steamurl": steamurl,
            "steamid": steamid,
            "avatar": avatar,
            "stats": stats,
            'updatedat': updatedat,
            'names': names,
            'gamebanned': gamebanned,
            'gamebans': gamebans,
            'relatedplayers': '',
            'checkedforalts': False
        }
        if fromdb and lastupdate < weekago:
            await db.update_playerinfo(playerinfo)
        else:
            await db.add_playerinfo(playerinfo)

        return playerinfo

    async def search_bm(self, steamid):
        """Takes a steam ID and converts it into a BM id for use."""
        url_extension = f"players?filter[search]={steamid}&include=identifier"
        url = f"{self.url_base}{url_extension}"
        my_headers = {"Authorization": self.bmtoken}
        response = ""
        async with aiohttp.ClientSession(headers=my_headers) as session:
            async with session.get(url=url) as r:
                response = await r.json()
        data = response
        return data["data"][0]["id"] if data["data"] else ""

    async def get_id_from_steam(self, url):
        """Takes the URL (well part of it) and returns a steam ID"""

        url = (
            f"https://api.steampowered.com/ISteamUser/ResolveVanityURL/v1/?format=json&"
            f"key={self.steamtoken}&vanityurl={url}&url_type=1"
        )
        data = ""
        async with aiohttp.ClientSession(
            headers={"Authorization": self.steamtoken}
        ) as session:

            async with session.get(url=url) as r:
                response = await r.json()
        data = response
        return data["response"]["steamid"] if data["response"]["steamid"] else 0

    async def gamebans(self, steamid):
        db = DataBase()
        gamebanned = await db.get_gameban(steamid=steamid)
        weekago = datetime.datetime.now(
            timezone.utc) - datetime.timedelta(hours=168)
        weekago = str(weekago)
        weekago = weekago.split(" ")
        weekago = weekago[0]
        weekago = datetime.datetime.strptime(weekago, '%Y-%m-%d')
        fromdb = await db.get_playerinfo(steamid=steamid)
        lastupdate = ''
        if fromdb:
            lastupdate = fromdb['updatedat']
            lastupdate = lastupdate.split(" ")
            lastupdate = lastupdate[0]
            lastupdate = datetime.datetime.strptime(
                lastupdate, '%Y-%m-%d')
            if lastupdate > weekago:
                return gamebanned
        if gamebanned:
            return gamebanned
        """Connects to the rustbanned api and returns any gamebans the user has."""
        searchurl = "https://rustbanned.com/api/eac_ban_check_v2.php"
        payload = {"apikey": f"{self.rustbannedapi}", "steamid64": steamid}
        headers = {"user-agent": "Gnomes App"}
        response = ""
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(
                url=searchurl, params=payload, allow_redirects=True
            ) as r:
                response = await r.json()
        data = response
        data['response'][0]['eac_ban_count'] = int(
            data['response'][0]['eac_ban_count'])
        if data['response'][0]['eac_ban_count']:
            tweetid = data['response'][0]['url']
            tweetid = tweetid.split('/')
            tweetid = tweetid[5]
            gamebanned = await db.tweet_lookup(tweetid=tweetid)

        return gamebanned

    async def kda_day(self, bmid):
        dayago = datetime.datetime.now(
            timezone.utc) - datetime.timedelta(hours=24)
        dayago = str(dayago).replace("+00:00", "Z:")
        dayago = dayago.replace(" ", "T")
        url_extension = (
            f"activity?version=^0.1.0&tagTypeMode=and"
            f"&filter[timestamp]={dayago}"
            f"&filter[types][whitelist]=rustLog:playerDeath:PVP,"
            f"rustLog:playerDeath:cold,"
            f"rustLog:playerDeath:died,"
            f"rustLog:playerDeath:fall,"
            f"rustLog:playerDeath:blunt,"
            f"rustLog:playerDeath:entity,"
            f"rustLog:playerDeath:drowned,"
            f"rustLog:playerDeath:suicide,"
            f"rustLog:playerDeath:bleeding"
            f"&filter[players]={bmid}&include=organization,user&page[size]=100"
        )
        url = f"{self.url_base}{url_extension}"
        my_headers = {"Authorization": self.bmtoken}
        response = ""
        async with aiohttp.ClientSession(headers=my_headers) as session:
            async with session.get(url=url) as r:
                response = await r.json()
        data = response
        return data

    async def kda_week(self, bmid):
        weekago = datetime.datetime.now(
            timezone.utc) - datetime.timedelta(hours=168)
        weekago = str(weekago).replace("+00:00", "Z:")
        weekago = weekago.replace(" ", "T")
        url_extension = (
            f"activity?version=^0.1.0&tagTypeMode=and"
            f"&filter[timestamp]={weekago}"
            f"&filter[types][whitelist]=rustLog:playerDeath:PVP,"
            f"rustLog:playerDeath:cold,"
            f"rustLog:playerDeath:died,"
            f"rustLog:playerDeath:fall,"
            f"rustLog:playerDeath:blunt,"
            f"rustLog:playerDeath:entity,"
            f"rustLog:playerDeath:drowned,"
            f"rustLog:playerDeath:suicide,"
            f"rustLog:playerDeath:bleeding"
            f"&filter[players]={bmid}&include=organization,user&page[size]=100"
        )
        url = f"{self.url_base}{url_extension}"
        my_headers = {"Authorization": self.bmtoken}
        response = ""
        async with aiohttp.ClientSession(headers=my_headers) as session:
            async with session.get(url=url) as r:
                response = await r.json()
        data = response
        return data

    async def additional_data(self, extension: str):
        response = ""
        async with aiohttp.ClientSession(
            headers={"Authorization": self.bmtoken}
        ) as session:
            async with session.get(url=extension) as r:
                response = await r.json()
        data = response
        return data

    async def search_ip(self, ip):
        url = f"https://vpnapi.io/api/{ip}?key={self.vpntoken}"
        response = ""
        async with aiohttp.ClientSession() as session:
            async with session.get(url=url) as r:
                response = await r.json()
        data = response
        return data


class DataBase():
    def __init__(self):
        self.bmapi = BMAPI()
        self.twitter_token = "AAAAAAAAAAAAAAAAAAAAADFyhQEAAAAATVV0WVBDvK9z8L6K8nzY4dxuPHw%3D7dqMVc3VO71FynV2yxOoCw5MwvSAbEsK0JnLr94vLe85Pbyvpv"
        self.author_id = 3243246400

    def bearer_oauth(self, r):
        r.headers["Authorization"] = f"Bearer {self.twitter_token}"
        r.headers["User-Agent"] = "v2TweetLookupPython"
        return r

    async def tweet_lookup(self, tweetid):
        tweet_fields = "tweet.fields=created_at,entities"
        ids = f"ids={tweetid}"
        url = f"https://api.twitter.com/2/tweets?{ids}&{tweet_fields}"
        headers = {}
        headers['Authorization'] = f"Bearer {self.twitter_token}"
        headers['User-Agent'] = "v2TweetLookupPython"
        response = requests.request("GET", url, auth=self.bearer_oauth)
        response = response.json()
        response = response['data'][0]
        tweet = {}
        steamid = response['entities']['urls'][0]['expanded_url']
        steamurl = steamid
        steamid = steamid.split('/')
        steamid = steamid[4]
        tweet = {
            'tweetid': tweetid,
            'tweetdate': response['created_at'],
            'tweetmsg': response['text'],
            'tweetlink': f"https://twitter.com/rusthackreport/status/{tweetid}",
            'steamid': steamid,
            'steamurl': steamurl}
        await self.add_tweet(tweet)
        return tweet

    async def setupdb(self):
        """
        Sets up the database for future use.
        """
        async with aiosqlite.connect('./lib/bm.db') as db:
            cur = await db.cursor()

            await cur.execute('''CREATE TABLE IF NOT EXISTS playerprofile(
                bmid INT UNIQUE NOT NULL PRIMARY KEY,
                steamid INT UNIQUE NOT NULL,
                playername VARCHAR(255) NOT NULL,
                rusthours FLOAT,
                aimtrain FLOAT,
                steamurl VARCHAR(255),
                avatar VARCHAR(255),
                relatedplayers VARCHAR(255),
                updatedat VARCHAR(255),
                names VARCHAR(255),
                kills_week INT,
                kills_day INT,
                deaths_day INT,
                deaths_week INT,
                gamebanned BOOLEAN,
                checkedforalts BOOLEAN
                )''')

            await cur.execute('''CREATE TABLE IF NOT EXISTS notes(
                noteid INT NOT NULL PRIMARY KEY,
                bmid INT NOT NULL REFERENCES playerprofile(bmid),
                orgid INT,
                notemakerid INT,
                orgname VARCHAR(255),
                note VARCHAR(255),
                notemakername VARCHAR(255)
                )''')

            await cur.execute('''CREATE TABLE IF NOT EXISTS servers(
                serverid INT PRIMARY KEY,
                servername VARCHAR(255),
                orgid INT,
                orgname VARCHAR(255)
                )''')

            await cur.execute('''CREATE TABLE IF NOT EXISTS serverbans(
                bmid INT PRIMARY KEY,
                bandate VARCHAR(30),
                expires VARCHAR(30),
                steamid INT,
                banid INT,
                bannote VARCHAR(255),
                serverid INT,
                servername VARCHAR(255),
                banner VARCHAR(255),
                banreason VARCHAR(255)
                )''')
            await cur.execute('''CREATE TABLE IF NOT EXISTS twitter_reports(
                tweetid VARCHAR(100) PRIMARY KEY,
                tweetdate VARCHAR(50),
                tweetmsg VARCHAR(255),
                tweetlink VARCHAR(255),
                steamid VARCHAR(100),
                steamurl VARCHAR(100))''')

            await cur.execute('''CREATE TABLE IF NOT EXISTS isps(
                isp_id INT NOT NULL PRIMARY KEY,
                isp_name VARCHAR(255) NOT NULL,
                is_vpn BOOLEAN,
                country VARCHAR(25),
                ip VARCHAR(100),
                connectedto VARCHAR(255)
                )''')

            await db.commit()
            await cur.close()

    async def add_playerinfo(self, playerinfo) -> None:

        if not playerinfo['steamid']:
            print("No steam id")
            return
        if not playerinfo['bmid']:
            print("No bmid")
            return

        if playerinfo['stats'] == 'None':
            playerinfo['kills_week'] = 0
            playerinfo['kills_day'] = 0
            playerinfo['deaths_week'] = 0
            playerinfo['deaths_day'] = 0
        else:
            playerinfo['kills_week'] = int(
                playerinfo['stats']['kills_week'])
            playerinfo['kills_day'] = int(playerinfo['stats']['kills_day'])
            playerinfo['deaths_week'] = int(
                playerinfo['stats']['deaths_week'])
            playerinfo['deaths_day'] = int(
                playerinfo['stats']['deaths_day'])

        async with aiosqlite.connect('./lib/bm.db') as db:
            cur = await db.cursor()
            try:
                await cur.execute("INSERT INTO playerprofile VALUES (:bmid,:steamid,:playername,:rusthours,:aimtrain,:steamurl,:avatar,:relatedplayers,:updatedat,:names,:kills_week,:kills_day,:deaths_day,:deaths_week,:gamebanned, :checkedforalts)",
                                  ({**playerinfo}))
                await db.commit()
                await cur.close()
            except aiosqlite.Error as e:
                myerror = str(e)
                await cur.close()
                if "UNIQUE" in myerror:
                    error = myerror.split(" ")
                    print(
                        f"The unique field '{error[3]}' is already associated with the value you submitted.")
                    return myerror
                else:
                    print(e)
                    return e

    async def update_playerinfo(self, playerinfo: dict):
        async with aiosqlite.connect('./lib/bm.db') as db:
            cur = await db.cursor()
            if playerinfo['stats'] == 'None':
                playerinfo['kills_week'] = 0
                playerinfo['kills_day'] = 0
                playerinfo['deaths_week'] = 0
                playerinfo['deaths_day'] = 0
            else:
                playerinfo['kills_week'] = int(
                    playerinfo['stats']['kills_week'])
                playerinfo['kills_day'] = int(playerinfo['stats']['kills_day'])
                playerinfo['deaths_week'] = int(
                    playerinfo['stats']['deaths_week'])
                playerinfo['deaths_day'] = int(
                    playerinfo['stats']['deaths_day'])
            try:
                await cur.execute('''UPDATE playerprofile
                              SET rusthours=:rusthours,
                              aimtrain=:aimtrain,
                              relatedplayers=:relatedplayers,
                              names=:names,
                              kills_week=:kills_week,
                              kills_day=:kills_day,
                              deaths_week=:deaths_week,
                              deaths_day=:deaths_day,
                              gamebanned=:gamebanned,
                              updatedat=:updatedat,
                              checkedforalts=:checkedforalts
                              WHERE bmid=:bmid''',
                                  ({**playerinfo}))
            except aiosqlite.Error as e:
                theerror = {'error': e}
                print("There's an error!")
                print(theerror)
                print("The error has ended!")
                return theerror
            await db.commit()
            await cur.close()

    async def update_relatedplayers(self, relatedplayers, bmid):

        relatedplayer = ''
        for i in relatedplayers:
            if relatedplayer:
                relatedplayer += f",{i}"
            else:
                relatedplayer = f"{i}"

        async with aiosqlite.connect('./lib/bm.db') as db:
            cur = await db.cursor()
            await cur.execute("UPDATE playerprofile SET relatedplayers = :relatedplayer, checkedforalts = :checkedforalts WHERE bmid=:bmid", ({'relatedplayer': relatedplayer, 'bmid': bmid, 'checkedforalts': True}))
            await db.commit()
            await cur.close()

    async def search_url(self, url):
        response = ""
        async with aiohttp.ClientSession() as session:
            async with session.get(url, allow_redirects=False) as response:
                response = str(response).split(
                    "Location': \'")[1].split("\'")[0]

        return response

    async def get_playerinfo(self, steamid: int = 0, bmid: int = 0):
        async with aiosqlite.connect('./lib/bm.db') as db:
            cur = await db.cursor()
            results = {}
            if steamid:
                try:
                    await cur.execute('''SELECT * FROM playerprofile WHERE steamid=:steamid''', ({'steamid': steamid}))
                    async for row in cur:
                        results = {
                            'bmid': row[0],
                            'steamid': row[1],
                            'playername': row[2],
                            'rusthours': row[3],
                            'aimtrain': row[4],
                            'avatar': row[6],
                            'steamurl': row[5],
                            'relatedplayers': row[7],
                            'updatedat': row[8],
                            'names': row[9],

                            'stats': {
                                'kills_day': row[11],
                                'kills_week': row[10],
                                'deaths_day': row[12],
                                'deaths_week': row[13]
                            },
                            'gamebanned': row[14],
                            'checkedforalts': row[15]
                        }
                except aiosqlite.Error as e:
                    theerror = {'error': e}
                    return theerror
            if bmid:
                try:
                    await cur.execute('''SELECT * FROM playerprofile WHERE bmid=:bmid''', ({'bmid': bmid}))
                    async for row in cur:
                        results = {
                            'bmid': row[0],
                            'steamid': row[1],
                            'playername': row[2],
                            'rusthours': row[3],
                            'aimtrain': row[4],
                            'avatar': row[6],
                            'steamurl': row[5],
                            'relatedplayers': row[7],
                            'updatedat': row[8],
                            'names': row[9],

                            'stats': {
                                'kills_day': row[11],
                                'kills_week': row[10],
                                'deaths_day': row[12],
                                'deaths_week': row[13]
                            },
                            'gamebanned': row[14],
                            'checkedforalts': row[15]
                        }
                except aiosqlite.Error as e:
                    theerror = {'error': e}
                    return theerror
            return results

    async def add_tweet(self, tweet):
        """Inserts the tweet info into the database.

        Args:
            tweetid, tweetdate, tweetmsg, tweetlink, steamid, steamurl

        Returns:
            Returns nothing or an error.
        """
        async with aiosqlite.connect('./lib/bm.db') as db:
            cur = await db.cursor()
            steamid = ''
            try:
                await cur.execute("INSERT INTO twitter_reports VALUES (:tweetid,:tweetdate,:tweetmsg,:tweetlink,:steamid,:steamurl)", ({**tweet}))
            except:
                print("Tweet already in database.")
            await db.commit()
            await cur.close()

    async def jsonify_tweets(self):
        async with aiosqlite.connect('./lib/bm.db') as db:
            cur = await db.cursor()
            res = await cur.execute("SELECT * FROM twitter_reports")
            myresults = await res.fetchall()
            count = 0
            file_count = 0
            mydict = {}
            totaltweets = 0
            for i in myresults:
                tweetid = str(i[0])
                tweetdate = str(i[1])
                tweetmsg = i[2]
                tweeturl = i[3]
                steamid = i[4]
                steamurl = i[5]
                mydict[tweetid] = {
                    'tweetid': tweetid,
                    'tweet': tweetmsg,
                    'date': tweetdate,
                    'link': tweeturl,
                    'steamid': steamid,
                    'steamurl': steamurl
                }
                count += 1
                if count == 50000:
                    totaltweets += len(mydict)
                    with open(f'./tweets/tweets_{file_count}.json', 'w') as f:
                        f.write(json.dumps(mydict, indent=4))
                    mydict = {}
                    file_count += 1
                    count = 0
            totaltweets += len(mydict)
            with open(f'./tweets/tweets_{file_count}.json', 'w') as f:
                f.write(json.dumps(mydict, indent=4))
            print(totaltweets)

    async def get_serverinfo(self, serverid):
        async with aiosqlite.connect('./lib/bm.db') as db:
            cur = await db.cursor()
            await cur.execute(
                "SELECT * FROM servers WHERE serverid = :serverid", ({'serverid': serverid}))
            servers = {}
            async for row in cur:
                servers = {
                    'serverid': serverid,
                    'servername': row[1],
                    'orgid': row[2],
                    'orgname': row[3]
                }
            return servers

    async def add_serverinfo(self, serverinfo):
        async with aiosqlite.connect('./lib/bm.db') as db:
            cur = await db.cursor()
            try:
                await cur.execute("INSERT INTO servers (serverid, servername, orgid, orgname) VALUES(:serverid, :servername, :orgid, :orgname)", ({**serverinfo}))
                await db.commit()
            except aiosqlite.Error as e:
                print(e)
        await cur.close()
        return

    async def update_serverinfo(self, serverinfo):
        async with aiosqlite.connect('./lib/bm.db') as db:
            cur = await db.cursor()
            await cur.execute("UPDATE servers SET servername = :servername, orgname = :orgname WHERE serverid = :serverid", ({**serverinfo}))
            await db.commit()
        await cur.close()
        return

    async def add_notes(self, notes):
        async with aiosqlite.connect('./lib/bm.db') as db:
            cur = await db.cursor()
            for i in notes:
                note = {
                    'noteid': notes[i]['noteid'],
                    'orgid': notes[i]['organization_id'],
                    'bmid': notes[i]['bmid'],
                    'notemakerid': notes[i]['notemaker_id'],
                    'note': notes[i]['note'],
                    'orgname': notes[i]['organization_name'],
                    'notemakername': notes[i]['notemaker_name']
                }
                try:
                    await cur.execute("INSERT INTO notes (noteid, bmid, orgid, notemakerid, orgname, note, notemakername) VALUES(:noteid, :orgid, :bmid, :notemakerid, :note, :orgname, :notemakername)", ({**note}))
                    await db.commit()
                except aiosqlite.Error as e:
                    print(e)
            await cur.close()
            return

    async def get_notes(self, bmid):
        async with aiosqlite.connect('./lib/bm.db') as db:
            cur = await db.cursor()
            await cur.execute("SELECT * FROM notes WHERE bmid = :bmid", ({'bmid': bmid}))
            notes = {}
            async for row in cur:
                notes[row[0]] = {
                    "noteid": row[0],
                    "organization_id": row[2],
                    "notemaker_id": row[3],
                    "organization_name": row[4],
                    "notemaker_name": row[6],
                    "note": row[5],
                    "bmid": bmid
                }
            return notes

    async def get_ipinfo(self, ip):
        results = ''
        ipinfo = {}
        async with aiosqlite.connect('./lib/bm.db') as db:
            cur = await db.cursor()
            await cur.execute("SELECT * FROM isps WHERE ip = :ip", ({"ip": ip}))
            results = await cur.fetchone()
        if results:
            ipinfo = {
                'ip': ip,
                'isp_id': results[0],
                'isp_name': results[1],
                'is_vpn': results[2],
                'country': results[3],
                'connectedto': results[4]
            }
        return ipinfo

    async def get_ips(self):
        results = ''
        async with aiosqlite.connect('./lib/bm.db') as db:
            cur = await db.cursor()
            await cur.execute("SELECT * FROM isps")
            results = await cur.fetchall()
        ips = {}
        for i in results:
            ips[i[4]] = {}
            ips[i[4]] = {
                'ip': i[4],
                'isp_id': i[0],
                'isp_name': i[1],
                'is_vpn': i[2],
                'country': i[3],
                'connectedto': i[5]
            }
        return ips

    async def populatetweets(self):
        async with aiosqlite.connect('./lib/bm.db') as db:
            myfiles = glob.glob('./tweets/*.json')
            cur = await db.cursor()
            for i in myfiles:
                tweets = {}
                with open(i, 'r') as f:
                    tweets = json.load(f)
                for i in tweets:
                    tweetid = str(tweets[i]['tweetid'])
                    tweetdate = str(tweets[i]['date'])
                    tweetmsg = str(tweets[i]['tweet'])
                    tweetlink = str(tweets[i]['link'])
                    steamid = str(tweets[i]['steamid'])
                    steamurl = str(tweets[i]['steamurl'])
                    try:
                        await cur.execute("INSERT INTO twitter_reports VALUES (?,?,?,?,?,?)",
                                          (tweetid, tweetdate, tweetmsg, tweetlink, steamid, steamurl))
                        print(f"Inserted to database:", tweetid, tweetdate,
                              tweetmsg, tweetlink, steamid, steamurl)
                    except:
                        print("Tweet already in database.")

                await db.commit()
            await cur.close()

    async def add_serverinfo(self, serverinfo):
        async with aiosqlite.connect('./lib/bm.db') as db:
            cur = await db.cursor()
            try:
                await cur.execute("INSERT INTO servers (serverid, servername, orgid, orgname) VALUES(:serverid, :servername, :orgid, :orgname)", ({**serverinfo}))
                await db.commit()
            except aiosqlite.Error as e:
                print(e)
        await cur.close()
        return

    async def add_ipinfo(self, ipinfo):
        for i in ipinfo:
            theip = ipinfo[i]
            ct = ''
            for j in theip['connectedto']:
                if ct:
                    ct += f",{j}"
                else:
                    ct = f"{j}"
            theip['connectedto'] = ct
            async with aiosqlite.connect('./lib/bm.db') as db:
                cur = await db.cursor()
                await cur.execute("INSERT INTO isps (isp_id, isp_name, is_vpn, country, ip, connectedto) VALUES(:isp_id, :isp_name, :is_vpn, :country, :ip, :connectedto)", ({**theip}))
                await db.commit()
                await cur.close()
        return

    async def update_ipinfo(self, ipinfo):
        async with aiosqlite.connect('./lib/bm.db') as db:
            cur = await db.cursor()
            for i in ipinfo:
                connectedto = i['connectedto']
                ct = ''
                for j in connectedto:
                    if ct:
                        ct += f",{j}"
                    else:
                        ct = f"{j}"
                i['connectedto'] = ct

                await cur.execute("UPDATE isps SET connectedto = :connectedto WHERE ip = :ip", ({**i}))
                await db.commit()
        await cur.close()

    async def get_gameban(self, steamid):
        results = ''
        async with aiosqlite.connect('./lib/bm.db') as db:
            cur = await db.cursor()
            await cur.execute("SELECT * FROM twitter_reports WHERE steamid = :steamid", ({'steamid': str(steamid)}))
            results = await cur.fetchone()
        gamebans = {}
        if results:
            gamebans = {
                'tweetid': results[0],
                'tweetdate': results[1],
                'tweetmsg': results[2],
                'tweetlink': results[3],
                'steamid': results[4],
                'steamurl': results[5]
            }
        return gamebans