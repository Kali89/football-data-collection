import json
import sqlite3
import os
import re
import xml.etree.ElementTree as ET
import collections
from datetime import datetime
from datetime import timedelta
from time import strptime
import time
import operator
import sys
import string
from dateutil.parser import parse
import psycopg2

#Player files to work with
matchFileDirectory = '/Users/msharpe/src/personal/football-data-collection/footballData/footballData/spiders/matches'
db = '/Users/matt/src/soccer/soccer_database.sqlite'
errorFile = matchFileDirectory + '/match_error.txt'
startIntFifa = 154994
startDateFifa = datetime(2007,2,22)
conn = psycopg2.connect("dbname=football")
cur = conn.cursor()
count = 0

def printError(country,season,id,count, filename):
    outputFile = open(errorFile,'a')
    outputFile.write(str(country) + ',' + str(season) + ',' + str(id) + ',' + str(filename) + '\n')
    outputFile.close()
    return count

def saveMatch(dirname,filename,count):
    thefile = os.path.join(dirname,filename)
    with open(thefile, 'r') as fh:
        print("Opened {0}".format(filename))
        xmlstr = fh.read()

        if xmlstr.find('</items>') > -1:
            xmlstr = xmlstr[:-8]
            print(xmlstr)
        if xmlstr.find('<>'):
            xmlstr = xmlstr.replace('<>', '<value>').replace('</>', '</value>')
        try:
            parsedXML = ET.fromstring(xmlstr)
            lstHomePlayerId = parsedXML.findall('homePlayersId/value')
            lstAwayPlayerId = parsedXML.findall('awayPlayersId/value')
            country = parsedXML.find('country').text
            season = parsedXML.find('season').text
            league = country + ' ' + parsedXML.find('league').text
            stage = parsedXML.find('stage').text
            matchApiId = parsedXML.find('matchId').text
            homeTeamApiId = int(parsedXML.find('homeTeamId/value').text)
            awayTeamApiId = int(parsedXML.find('awayTeamId/value').text)
            homeTeamFullName = parsedXML.find('homeTeamFullName/value').text
            awayTeamFullName = parsedXML.find('awayTeamFullName/value').text
            homeTeamAcronym = parsedXML.find('homeTeamAcronym/value').text
            awayTeamAcronym = parsedXML.find('awayTeamAcronym/value').text
            try:
                homeTeamGoal = int(parsedXML.find('homeTeamGoal').text)
                awayTeamGoal = int(parsedXML.find('awayTeamGoal').text)
            except:
                homeTeamGoal = -1
                awayTeamGoal = -1

            matchDateStr = parsedXML.find('date').text
            month_part, year = matchDateStr.split()
            month, day= month_part.split('/')
            matchYear = int(year)
            matchMonth = int(month)
            matchDay = int(day)
            matchDate = datetime(matchYear,matchMonth,matchDay)
        except:
            return printError('None', 'None', thefile, count, filename)
        country_id = get_item(
            "INSERT INTO Country (name) VALUES ( '%s' )" %  country,
            "SELECT id FROM Country WHERE name = '%s' " % country,
            cur
        )
#        cur.execute('''INSERT INTO Country (name) VALUES ( %s )''', ( country, ))
#        cur.execute('SELECT id FROM Country WHERE name = %s ', (country, ))
#        country_id = cur.fetchone()[0]
        league_id = get_item(
            "INSERT INTO League (country_id, name) VALUES ( '%s', '%s' )" % (country_id, league),
            "SELECT id FROM League WHERE name = '%s' " % league,
            cur
        )
#        cur.execute('''INSERT INTO League (country_id, name)
#                    VALUES ( %s, %s )''', (country_id, league, ) )
#        cur.execute('SELECT id FROM League WHERE name = %s ', (league, ))
#        league_id = cur.fetchone()[0]
        home_team_api_id = get_item(
            "INSERT INTO Team (team_api_id,team_long_name,team_short_name) VALUES ( '%s', '%s', '%s')" % (homeTeamApiId, homeTeamFullName,homeTeamAcronym),
            "SELECT team_api_id FROM Team WHERE team_api_id = '%s' " % homeTeamApiId,
            cur
        )
#        cur.execute('''INSERT INTO Team (team_api_id,team_long_name,team_short_name)
#                        VALUES ( %s, %s, %s)''', (homeTeamApiId, homeTeamFullName,homeTeamAcronym,))
#        cur.execute('SELECT team_api_id FROM Team WHERE team_api_id = %s ', (homeTeamApiId, ))
#        home_team_api_id = cur.fetchone()[0]
        away_team_api_id = get_item(
            "INSERT INTO Team (team_api_id,team_long_name,team_short_name) VALUES ( '%s', '%s', '%s' )" % (awayTeamApiId, awayTeamFullName,awayTeamAcronym),
            "SELECT team_api_id FROM Team WHERE team_api_id = '%s' " % (awayTeamApiId ),
            cur
        )
#        cur.execute('''INSERT INTO Team (team_api_id,team_long_name,team_short_name)
#                        VALUES ( %s, %s, %s )''', (awayTeamApiId, awayTeamFullName,awayTeamAcronym,))
#        cur.execute('SELECT team_api_id FROM Team WHERE team_api_id = %s ', (awayTeamApiId, ))
#        away_team_api_id = cur.fetchone()[0]


        cur.execute("INSERT INTO Match (country_id,league_id,season,stage,date, match_api_id, home_team_api_id, away_team_api_id, home_team_goal,away_team_goal) VALUES ( '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % ( country_id, league_id,season,stage,matchDate, matchApiId,home_team_api_id,away_team_api_id,homeTeamGoal,awayTeamGoal))
        cur.execute("SELECT id FROM Match WHERE match_api_id = '%s' " % matchApiId)
        match_id = cur.fetchone()[0]
        conn.commit()

        try:
            lstHomePlayerX = parsedXML.findall('homePlayersX/value')
            lstHomePlayerY = parsedXML.findall('homePlayersY/value')
            lstAwayPlayerX = parsedXML.findall('homePlayersX/value')
            lstAwayPlayerY = parsedXML.findall('homePlayersY/value')
        except:
            pass

        try:
            goal = parsedXML.find('goal')
            goaltxt = ET.tostring(goal)
            cur.execute("Update Match SET goal='%s' WHERE match_api_id='%s'" % (goaltxt,matchApiId))
            conn.commit()
        except:
            pass

        try:
            shoton = parsedXML.find('shoton')
            shotontxt = ET.tostring(shoton)
            cur.execute("Update Match SET shoton='%s' WHERE match_api_id='%s'" % (shotontxt,matchApiId))
            conn.commit()
        except:
            pass

        try:
            shotoff = parsedXML.find('shotoff')
            shotofftxt = ET.tostring(shotoff)
            cur.execute("Update Match SET shotoff='%s' WHERE match_api_id='%s'" % (shotofftxt,matchApiId))
            conn.commit()
        except:
            pass

        try:
            foulcommit = parsedXML.find('foulcommit')
            foulcommittxt = ET.tostring(foulcommit)
            cur.execute("Update Match SET foulcommit='%s' WHERE match_api_id='%s'" % (foulcommittxt,matchApiId))
            conn.commit()
        except:
            pass

        try:
            card = parsedXML.find('card')
            cardtxt = ET.tostring(card)
            cur.execute("Update Match SET card='%s' WHERE match_api_id='%s'" % (cardtxt,matchApiId))
            conn.commit()
        except:
            pass

        try:
            cross = parsedXML.find('cross')
            crosstxt = ET.tostring(cross)
            cur.execute("Update Match SET crosses='%s' WHERE match_api_id='%s'" % (crosstxt, matchApiId))
            conn.commit()
        except:
            pass

        try:
            corner = parsedXML.find('corner')
            cornertxt = ET.tostring(corner)
            cur.execute("Update Match SET corner='%s' WHERE match_api_id='%s'" % (cornertxt,matchApiId))
            conn.commit()
        except:
            pass

        try:
            possession = parsedXML.find('possession')
            possessiontxt = ET.tostring(possession)
            cur.execute(
                "Update Match SET possession='%s' WHERE match_api_id='%s'" % (
                   possessiontxt, matchApiId
                )
            )
            conn.commit()
        except:
            pass

        #Squad parsing
#        for i in range(0,11):
#
#                try:
#                    homePlayerApiId = int(lstHomePlayerId[i].text)
#                    awayPlayerApiId = int(lstAwayPlayerId[i].text)
#                except:
#                    return printError(country,season,matchApiId,count, filename)
#
#                try:
#                    cur.execute('SELECT id FROM Player WHERE player_api_id = %s ', (homePlayerApiId, ))
#                    home_player_id = cur.fetchone()[0]
#                    cur.execute('Update Match SET home_player_' + str(i+1) + '=%s WHERE match_api_id=%s', (homePlayerApiId,matchApiId))
#                except:
#                    pass
#
#                try:
#                    cur.execute('SELECT id FROM Player WHERE player_api_id = %s ', (awayPlayerApiId, ))
#                    away_player_id = cur.fetchone()[0]
#                    cur.execute('Update Match SET away_player_' + str(i+1) + '=%s WHERE match_api_id=%s', (awayPlayerApiId,matchApiId))
#                except:
#                    pass
#
#                try:
#                    cur.execute('Update Match SET home_player_X' + str(i+1) + '=%s WHERE match_api_id=%s', (int(lstHomePlayerX[i].text),matchApiId))
#                    cur.execute('Update Match SET home_player_Y' + str(i+1) + '=%s WHERE match_api_id=%s', (int(lstHomePlayerY[i].text),matchApiId))
#                    cur.execute('Update Match SET away_player_X' + str(i+1) + '=%s WHERE match_api_id=%s', (int(lstAwayPlayerX[i].text),matchApiId))
#                    cur.execute('Update Match SET away_player_Y' + str(i+1) + '=%s WHERE match_api_id=%s', (int(lstAwayPlayerY[i].text),matchApiId))
#                except:
#                    pass

    conn.commit()
    print ("Saved match #" + str(count))
    count += 1
    return count


def get_item(insert_sql, get_sql, cur):
    cur.execute(get_sql)
    thing = cur.fetchone()
    if thing:
        return thing[0]
    else:
        cur.execute(insert_sql)
        cur.execute(get_sql)
        id = cur.fetchone()[0]
        return id


print ("Match lookup started...")

for (dirname, dirs, files) in os.walk(matchFileDirectory):
    for filename in files:
        if filename.endswith('.xml'):
            count = saveMatch(dirname,filename,count)
