let team_mapping = require("./team_mapping.js")
let info_getter = require("./tournament_info_getter.js")
let path = require("path")

function build_slug(competition_short_name, season, round, index ){  
  return competition_short_name + season.replace('/','') + "-" + round.toLowerCase() + "-" + index.padStart(3, "0")
}

function build_slug_from_data_from_match(match_info){
  return build_slug(match_info.competition_short_name,match_info.season,match_info.round,match_info.index)
}

async function fetch_and_create_metadata(comp_id,date,home_team,away_team,asset_type) {  
  let match_element = {}
  match_element = await fetch_and_populate_metadata(match_element,comp_id,date,home_team,away_team,asset_type)  
  return match_element
}

async function fetch_and_populate_metadata(match_element,comp_id,date,home_team,away_team,title_type) {
  let match_info = await info_getter.getDataForMatch(comp_id,date,home_team,away_team)
  // chl | chc
  match_info.competition_short_name = team_mapping.adapt_competition(match_info.competition_short_name)
  // 
  let slug = null
  
  slug = build_slug_from_data_from_match(match_info)
  // match_element.match_id = slug
  
  if (match_element.public == null) {
    match_element.public = {}
  }
  if (match_element.public.asset_metadata == null) {
    match_element.public.asset_metadata = {}
  }
  match_element.public.asset_metadata.slug = slug
  // ADM - removed from metadata since it's alraedy in info.match_id
  // match_element.public.asset_metadata.match_id = slug

  if (title_type == null) {
    match_element.public.asset_metadata.asset_type = "match"
  }
  if (match_element.public.asset_metadata.info == null) {
    match_element.public.asset_metadata.info = {}
  }
  match_element.date = date

  let asset_metadata = match_element.public.asset_metadata
  let info = asset_metadata.info
  info.match_id = slug
  info.team_home_name = match_info.home_team
  info.team_away_name = match_info.away_team
  info.team_home_code = team_mapping.find_epcr_team_code(info.team_home_name)
  info.team_away_code = team_mapping.find_epcr_team_code(info.team_away_name)
  info.tournament_season = match_info.season
  info.tournament_stage_short = team_mapping.find_round_short_name(match_info.round)
  info.tournament_stage = team_mapping.find_round_name(info.tournament_stage_short)
  info.start_time = match_info.time
  info.date = match_info.date
  info.tournament_name = team_mapping.find_competition_name(match_info.competition_short_name)
  info.tournament_id = match_info.competition_short_name

  asset_metadata.ip_title_id = adapt_slug_to_title_type(slug,title_type)
  asset_metadata.slug = asset_metadata.ip_title_id
  asset_metadata.title_type = normalize_title_type(title_type)
  asset_metadata.asset_type = get_asset_type(asset_metadata.title_type)

  asset_metadata.display_title = compute_display_title(match_element,title_type)  
  return match_element
}

function adapt_slug_to_title_type(slug,title_type){
  if (title_type == null || title_type.toLowerCase().includes("match")){
    return slug
  } else {
    return slug + "-" + title_type.toLowerCase()
  }
}

function compute_display_title(match_info,title_type){
  // EPCR Champions Cup - 2023-2024 - R1 - Glasgow Warriors v Northampton Saints
  if (title_type.toLowerCase() != "match") {
      return team_mapping.find_competition_name(match_info.public.asset_metadata.info.tournament_id) + " - " + match_info.public.asset_metadata.info.tournament_season 
    + " - " + match_info.public.asset_metadata.info.tournament_stage_short + " - " + match_info.public.asset_metadata.info.team_home_name + " v " + match_info.public.asset_metadata.info.team_away_name + " - " + normalize_title_type(title_type);

  }else{
    return team_mapping.find_competition_name(match_info.public.asset_metadata.info.tournament_id) + " - " + match_info.public.asset_metadata.info.tournament_season 
      + " - " + match_info.public.asset_metadata.info.tournament_stage_short + " - " + match_info.public.asset_metadata.info.team_home_name + " v " + match_info.public.asset_metadata.info.team_away_name;
  }
}

/** public.asset_metadata.title = ??????? - Match - 2023-12-08 - chp202324-r1-001 - Glasgow Warriors v Northampton Saints 
 * // 2025/03/12 - ADM - Changed to 2023-12-08 - chp202324-r1-001 - Glasgow Warriors v Northampton Saints - Match - ??????? as per EPCR request
*/
function compute_title_common(date,match_id,team_home_name,team_away_name,title_type){
  return date + " - " + match_id + " - " + team_home_name + " v " + team_away_name + " - " + title_type.toUpperCase() ;
}

/** public.asset_metadata.title = VOD - Match - 2023-12-08 - chp202324-r1-001 - Glasgow Warriors v Northampton Saints 
 * // 2025/03/12 - ADM - Changed to 2023-12-08 - chp202324-r1-001 - Glasgow Warriors v Northampton Saints - Match - VOD as per EPCR request
*/
function compute_title_for_mezzanine_element(match_element){
  // return "VOD - " + compute_title_common(match_element)
  return compute_title_for_mezzanine(match_element.public.asset_metadata.info.date,match_element.public.asset_metadata.info.match_id,
    match_element.public.asset_metadata.info.team_home_name,match_element.public.asset_metadata.info.team_away_name,
    match_element.public.asset_metadata.title_type)
}

function compute_title_for_mezzanine(date,match_id,team_home_name,team_away_name,title_type){
  return compute_title_common(date,match_id,team_home_name,team_away_name,title_type) + " - VOD"
}

/** public.asset_metadata.title = MASTER - Match - 2023-12-08 - chp202324-r1-001 - Glasgow Warriors v Northampton Saints 
 * // 2025/03/12 - ADM - Changed to 2023-12-08 - chp202324-r1-001 - Glasgow Warriors v Northampton Saints - Match - MASTER as per EPCR request
*/
function compute_title_for_master_element(match_element){
  // return "MASTER - " + compute_title_common(match_element)
  return compute_title_for_master(match_element.public.asset_metadata.info.date,match_element.public.asset_metadata.info.match_id,
    match_element.public.asset_metadata.info.team_home_name,match_element.public.asset_metadata.info.team_away_name)
}

function compute_title_for_master(date,match_id,team_home_name,team_away_name){
  // return "MASTER - " + compute_title_common(match_element)
  return compute_title_common(date,match_id,team_home_name,team_away_name,"master")
}

// s3://epcrwbdarch/RGU_ECC_2022-04-09_NEW_V_ZEB_CLEAN_MATCH_FEED.mxf
// s3://epcrwbdarch/RGU_ECC_BAT_V_BRI_2016-10-20_MATCH_FEED.mxf
// returns comp_id,date,home_team,away_team,asset_type
// ERC champions cup
// ECC challenge cup
function parse_asset_file_name(file_name){
  if (file_name == "RGU_ECC_2022-04-09_NEW_V_ZEB_CLEAN_MATCH_FEED.mxf"){
    return [info_getter.challenge_cup_id,"2022-04-09","Newcastle Falcons","Zebre Parma","MATCH"]
  }
  if (file_name == "RGU_ECC_BOR_v_EDI_Match_HLs_Comp_2014-10-17.mxf"){
    return [info_getter.challenge_cup_id,"2014-10-17","Union Bordeaux-Begles","Edinburgh Rugby","MATCH"]
  }
  const file_match_regex = new RegExp("^RGU_(ERC|ECC)_(.*)_(v|V)_(.*)_(\\d\\d\\d\\d-\\d\\d-\\d\\d)_(.*)\\..*$")
  if (!file_match_regex.test(file_name)) {
    // SHOULD NEVER HAPPEN
    return [null,null,null,null,null]
  }
  matching_results = file_name.match(file_match_regex)
  return [info_getter.get_competition_id(matching_results[1]),
    matching_results[5],
    team_mapping.find_epcr_team_from_code(team_mapping.find_epcr_team_code_from_s3_code(matching_results[2])),
    team_mapping.find_epcr_team_from_code(team_mapping.find_epcr_team_code_from_s3_code(matching_results[4])),
    matching_results[6]]
}

async function fetch_and_create_metadata_from_s3(file_path){  
  const [comp_id,date,home_team,away_team,title_type] = parse_asset_file_name(path.basename(file_path))
  
  let metadata = await fetch_and_create_metadata(comp_id,date,home_team,away_team,title_type)
    
  metadata.public.asset_metadata.title_type = normalize_title_type(title_type)
  metadata.public.asset_metadata.asset_type = get_asset_type(metadata.public.asset_metadata.title_type)
  metadata.public.asset_metadata.title = compute_title_for_mezzanine_element(metadata)
  metadata.public.name = metadata.public.asset_metadata.title
  metadata.public.description = metadata.public.asset_metadata.display_title
  
  return metadata
}

function normalize_title_type(title_type) {
  const title_type_lower = title_type.toLowerCase()
  if (title_type == null || title_type_lower.includes("match") || title_type_lower.includes("feed")) {
    return "Match"
  }
  if (title_type_lower.includes("highlights")) {
    return "Highlights"
  }
  if (title_type_lower.includes("iso")) {
    return "ISO"
  }
  if (title_type_lower.includes("ob_evs_dump")) {
    return "ISO"
  }
  // Add more normalization rules as needed
  return title_type.toLowerCase()
} 

function get_asset_type(title_type) {
  if (title_type == "Match" ) {
    return "primary"
  }
  return "auxiliary"
}

exports.fetch_and_create_metadata_from_s3 = fetch_and_create_metadata_from_s3
exports.fetch_and_create_metadata = fetch_and_create_metadata

