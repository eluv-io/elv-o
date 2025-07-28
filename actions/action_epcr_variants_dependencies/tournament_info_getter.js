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
}

function get_competition_id(comp_short_name){
  switch (comp_short_name.toLowerCase()) {
    case champions_cup_short_name, champions_cup_short_name_alternative:
      return champions_cup_id      
    case challenge_cup_short_name, challenge_cup_short_name_alternative:
      return challenge_cup_id
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
                  team_mapping.find_epcr_team_name(item["awayTeam"]["name"])] // 7 - Away Team Name
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

async function getDataForMatch(comp_id,date,home_team,away_team){  
  let rows = [];
  let year = date
  let year_match = date.match(new RegExp(/(\d\d\d\d)-(\d\d)-\d\d/))
  if (year_match != null) {
    year = year_match[1]
    if (year_match[2] < "10") {
      year = "" + (parseInt(year) - 1)
    }
  }
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
        match_data.index = ""+(index+1) // this provides a unique identifier to the match
        return match_data
      }      
    }
    throw Error("Match not found")
  // })
}

exports.get_competition_short_name = get_competition_short_name
exports.get_competition_id = get_competition_id
exports.getDataForMatch = getDataForMatch
exports.getData = getData
exports.champions_cup_id = champions_cup_id
exports.challenge_cup_id = challenge_cup_id
exports.champtions_cup_name = champtions_cup_name
exports.challenge_cup_name = challenge_cup_name
exports.champions_cup_short_name = champions_cup_short_name
exports.challenge_cup_short_name = challenge_cup_short_name