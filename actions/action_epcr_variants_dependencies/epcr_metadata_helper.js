import team_mapping from "./team_mapping.js"
import info_getter from "./tournament_info_getter.js"
import path from "path"

function build_slug(competition_short_name, season, round, index ){  
  return competition_short_name + season.replace('/','') + "-" + round.toLowerCase() + "-" + index.padStart(3, "0")
}

function build_slug_from_data_from_match(match_info){
  return build_slug(match_info.competition_short_name,match_info.season,match_info.round,match_info.index)
}

async function fetch_and_create_metadata(comp_id,date,home_team,away_team,asset_type) {
  let match_element = {}
  await fetch_and_populate_metadata(match_element,comp_id,date,home_team,away_team,asset_type)
  return match_element
}

async function fetch_and_populate_metadata(match_element,comp_id,date,home_team,away_team,asset_type) {
  let match_info = await info_getter.getDataForMatch(comp_id,date,home_team,away_team)
  // chl | chc
  match_info.competition_short_name = team_mapping.adapt_competition(match_info.competition_short_name)
  // 
  let slug = null
  
  slug = build_slug_from_data_from_match(match_info)
  match_element.match_id = slug
  
  if (match_element.public == null) {
    match_element.public = {}
  }
  if (match_element.public.asset_metadata == null) {
    match_element.public.asset_metadata = {}
  }
  match_element.public.asset_metadata.slug = slug
  match_element.public.asset_metadata.match_id = slug

  if (asset_type == null) {
    match_element.public.asset_metadata.asset_type = "match"
  }
  if (match_element.public.asset_metadata.info == null) {
    match_element.public.asset_metadata.info = {}
  }
  match_element.date = date

  let asset_metadata = match_element.public.asset_metadata
  let info = asset_metadata.info
  asset_metadata.ip_title_id = slug
  asset_metadata.slug = slug
  info.match_id = slug
  info.team_home_name = match_info.home_team
  info.team_away_name = match_info.away_team
  info.team_home_code = team_mapping.find_epcr_team_code(info.team_home_name)
  info.team_away_code = team_mapping.find_epcr_team_code(info.team_away_name)
  info.tournament_season = match_info.season
  info.tournament_stage_short = team_mapping.find_round_short_name(match_info.round)
  info.tournament_stage = team_mapping.find_round_name(info.tournament_stage_short)
  info.date = match_info.date
  info.tournament_name = team_mapping.find_competition_name(match_info.competition_short_name)
  info.tournament_id = match_info.competition_short_name
  asset_metadata.display_title = compute_display_title(match_element)
  return match_element
}

function compute_display_title(match_info){
  // EPCR Champions Cup - 2023-2024 - R1 - Glasgow Warriors v Northampton Saints
  return team_mapping.find_competition_name(match_info.public.asset_metadata.info.tournament_id) + " - " + match_info.public.asset_metadata.info.tournament_season 
    + " - " + match_info.public.asset_metadata.info.tournament_stage_short + " - " + match_info.public.asset_metadata.info.team_home_name + " v " + match_info.public.asset_metadata.info.team_away_name;
}

/** public.asset_metadata.title = ??????? - Match - 2023-12-08 - chp202324-r1-001 - Glasgow Warriors v Northampton Saints 
 * // 2025/03/12 - ADM - Changed to 2023-12-08 - chp202324-r1-001 - Glasgow Warriors v Northampton Saints - Match - ??????? as per EPCR request
*/
function compute_title_common(date,match_id,team_home_name,team_away_name,asset_type){
  return date + " - " + match_id + " - " + team_home_name + " v " + team_away_name + " - " + asset_type.toUpperCase() ;
}

/** public.asset_metadata.title = VOD - Match - 2023-12-08 - chp202324-r1-001 - Glasgow Warriors v Northampton Saints 
 * // 2025/03/12 - ADM - Changed to 2023-12-08 - chp202324-r1-001 - Glasgow Warriors v Northampton Saints - Match - VOD as per EPCR request
*/
function compute_title_for_mezzanine_element(match_element){
  // return "VOD - " + compute_title_common(match_element)
  return compute_title_for_mezzanine(match_element.public.asset_metadata.info.date,match_element.public.asset_metadata.info.match_id,
    match_element.public.asset_metadata.info.team_home_name,match_element.public.asset_metadata.info.team_away_name,
    match_element.public.asset_metadata.asset_type)
}

function compute_title_for_mezzanine(date,match_id,team_home_name,team_away_name,asset_type){
  return compute_title_common(date,match_id,team_home_name,team_away_name,asset_type) + " - VOD"
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
    return info_getter.challenge_cup_id,"2022-04-09","Newcastle Falcons","Zebre Parma","MATCH"
  }
  const file_match_regex = new RegExp("^RGU_(ERC|ECC)_(.*)_V_(.*)_(\\d\\d\\d\\d-\\d\\d-\\d\\d)_(.*)/..*$")
  if (!file_match_regex.test(file_name)) {
    // SHOULD NEVER HAPPEN
    return null,null,null,null,null
  }
  matching_results = file_name.match(file_match_regex)
  return matching_results[1],matching_results[2],matching_results[3],matching_results[4],matching_results[5]
}

function fetch_and_create_metadata_from_s3(file_path){
  match_info = parse_asset_file_name(path.basename(file_path))
  return fetch_and_create_metadata(match_info)
}


export default {build_slug,build_slug_from_data_from_match,compute_title_for_master_element,compute_title_for_mezzanine_element,compute_title_for_master,compute_title_for_mezzanine,compute_display_title,fetch_and_create_metadata,parse_asset_file_name,fetch_and_create_metadata_from_s3}
