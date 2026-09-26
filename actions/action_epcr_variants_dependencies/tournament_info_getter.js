/**
 * Script to download matches information from api.rugbyviz.com for all seasons and all competitions.
 */

let https = require('node:https')
let util = require('util')
let fs = require('fs')
let { stringify } = require('csv-stringify/sync')
let { Hash } = require('node:crypto')
let team_mapping = require("./team_mapping.js")


const champions_cup_id = "1008"
const challenge_cup_id = "1026"

const champtions_cup_name = "EPCR Champions Cup"
const challenge_cup_name = "EPCR Challenge Cup"

const champions_cup_short_name = "chp"
const challenge_cup_short_name = "chl"

const champions_cup_short_name_alternative = "erc"
const challenge_cup_short_name_alternative = "ecc"


var username = "epcr_eluvio"; 
var password = "KB)cH&S";
var authenticationHeader = "Basic " + Buffer.from(username + ":" + password).toString("base64");

function split_date_time(date_time) {
  const values = date_time.split("T");
  values[1] = values[1].split(".")[0];
  return values;
}

function get_competition_short_name(comp_id){
  switch(comp_id){
    case champions_cup_id:
      return champions_cup_short_name;
    case challenge_cup_id:
      return challenge_cup_short_name;        
  }
  // unknown competition id, return as is
  return comp_id
}

function get_alternate_competition_short_name(comp_short_name){
  switch (comp_short_name.toLowerCase()) {
    case champions_cup_short_name:
    case champions_cup_short_name_alternative:
      return challenge_cup_short_name
    case challenge_cup_short_name:
    case challenge_cup_short_name_alternative:
      return champions_cup_short_name
  }
  return comp_short_name
}

function get_competition_id(comp_short_name){
  switch (comp_short_name.toLowerCase()) {
    case champions_cup_short_name:
    case champions_cup_short_name_alternative:
      return champions_cup_id      
    case challenge_cup_short_name:
    case challenge_cup_short_name_alternative:
      return challenge_cup_id
  }
  return comp_short_name
}

function adapt_competition_short_name(comp_short_name){
  switch (comp_short_name.toLowerCase()) {    
    case champions_cup_short_name_alternative:
      return champions_cup_short_name          
    case challenge_cup_short_name_alternative:
      return challenge_cup_short_name
  }
  return comp_short_name
}


function get_round(original_round,pool) {
  switch(pool){
    case "TF":
      return "F"
    case "SF":
    case "QF":
      return pool
    case "R16":
      return "RO16"
    case "R6":
      return "QF"
    case "R7":
      return "SF"
    case "R8":
      return "F"      
  }

  return "R"+original_round
}

 function getInfoPromise(rows,comp_id,year) {  
  let adapted_comp_id = get_competition_id(comp_id)
  
  // console.log("getInfoPromise for comp_id: " + adapted_comp_id + " year: " + year)
  let path = `/rugby/v1/match/search?compId=${adapted_comp_id}&seasonId=${year}01`
  let competition_name = get_competition_short_name(adapted_comp_id)
  let options = {
    hostname: 'api.rugbyviz.com',
    port: 443,
    path: path,
    method: 'GET',
    headers : { "Authorization" : authenticationHeader,
      accept: 'application/json'
     } 
  }

  return new Promise((resolve,reject) => {
      let body = '';

      const req = https.get(options, (res) => {
        // console.log('statusCode:', res.statusCode);
        // console.log('headers:', res.headers);  
      
        res.on('data', (d) => {
          body += d;    
        });
      
        res.on('end', () =>{          
          JSON.parse(body).forEach( item => {  
            if (item["matchStatus"] == "result") {
              let entry = [item["competition"]["name"], // 0 - Competition Name
                  split_date_time(item["dateTime"])[0], // 1 - Date
                  split_date_time(item["dateTime"])[1], // 2 - Time
                  item["season"]["name"], // 3 - Season
                  get_round(item["round"],item["title"]), // 4 - Round
                  item["title"], // 5 - Title
                  team_mapping.find_epcr_team_name(item["homeTeam"]["name"]), // 6 - Home Team Name
                  team_mapping.find_epcr_team_name(item["awayTeam"]["name"]), // 7 - Away Team Name
                  item["id"]] // 8 - Match ID
              rows.push(entry);
            } else {
              console.log("home team is null: match status = " + item["matchStatus"]);
            }
          });        
          resolve(rows);
        })

      });
            
      req.on('error', (e) => {
        console.error(e);
        reject(e);
      });
    });
  }

  /**
   * 
   * @returns all matches information retrieved for the season
   */
function getData(comp_id,year) {
  let rows = [];    
  getInfoPromise(rows,comp_id,year).then()
  return rows
  
}


/**
 *      
 * @returns the season year for the specified date. For example, if the date is 2024-06-15, the season year will be 2023, as we consider that the season starts in August and ends in July.
 */
function find_season_year(date) {
  const date_parser = /^([0-9]{4})-([0-9]{2})-([0-9]{2})$/.exec(date)
  let year = null
  if (date_parser) {
    year = date_parser[1]
    if (date_parser[2] <= "10") {
      year = date_parser[1] - 1
    }
  }
  return year
}


async function getDataForMatch(comp_id,date,home_team,away_team){  
  let rows = [];
  let year = find_season_year(date);

  await getInfoPromise(rows,comp_id,year)
  //.then( rows => {
    for (let index = 0; index < rows.length; index++) {
      const match = rows[index];
      if (match[1] == date && match[6] == home_team && match[7] == away_team){
        let match_data = {}
        match_data.date = match[1]
        match_data.time = match[2]
        match_data.season = match[3].replace("/","-20")
        match_data.competition_short_name = get_competition_short_name(comp_id)
        match_data.round = match[4]
        match_data.title = match[5]
        match_data.home_team = match[6]
        match_data.away_team = match[7]
        match_data.opta_id = match[8]
        match_data.index = ""+(index+1) // this provides a unique identifier to the match
        return match_data
      }      
    }
    return null
  // })
}

    async function getStartAndEndEventsForOptaID(match_time_events = {}, opta_id, authenticationHeader,reporter = null) {
        let path = `/rugby/v1/matchevents/${opta_id}?typeId=13`
        let options = {
            hostname: 'api.rugbyviz.com',
            port: 443,
            path: path,
            method: 'GET',
            headers: {
                "Authorization": authenticationHeader,
                accept: 'application/json'
            }
        }

        return new Promise((resolve, reject) => {
            let body = '';

            const req = https.get(options, (res) => {

                res.on('data', (d) => {
                    body += d;
                });

                res.on('end', () => {
                    JSON.parse(body).events.forEach((item, index, full_array) => {
                        let entry = {}
                        // "dateTime": "2025-06-14T16:00:00.000Z",
                        // propertyId 327 start period
                        // propertyId 328 end period
                        for (let i = 0; i < item["properties"].length; i++) {
                            if (item["properties"][i]["propertyId"] == 327 && item["period"]["id"] == 20) { // 20 -> first half
                                // match start event
                                match_time_events["start_event"] = {"timestamp": item["timestamp"], "minute": item["minute"], "second": item["second"]}
                                if (reporter) {
                                  reporter.reportProgress("Setting start_event " + match_time_events["start_event"])
                                }
                                
                            }
                            if (item["properties"][i]["propertyId"] == 328 && item["period"]["id"] == 150) { // 150 -> post match
                                // match end event
                                match_time_events["end_event"] = {"timestamp": item["timestamp"], "minute": item["minute"], "second": item["second"]}
                                if (reporter) {
                                  reporter.reportProgress("Setting end_event " + match_time_events["end_event"])
                                }
                                
                            }
                        }
                    });
                    resolve(match_time_events);
                })
            })

            req.on('error', (e) => {
                logger.Error(e);
                reject(e);
            })
        })
    }

exports.get_competition_short_name = get_competition_short_name
exports.adapt_competition_short_name = adapt_competition_short_name
exports.get_alternate_competition_short_name = get_alternate_competition_short_name
exports.get_competition_id = get_competition_id
exports.getDataForMatch = getDataForMatch
exports.getData = getData
exports.find_season_year = find_season_year
exports.getStartAndEndEventsForOptaID = getStartAndEndEventsForOptaID
exports.champions_cup_id = champions_cup_id
exports.challenge_cup_id = challenge_cup_id
exports.champtions_cup_name = champtions_cup_name
exports.challenge_cup_name = challenge_cup_name
exports.champions_cup_short_name = champions_cup_short_name
exports.challenge_cup_short_name = challenge_cup_short_name