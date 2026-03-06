let team_mapping = require("./team_mapping.js")
let info_getter = require("./tournament_info_getter.js")
let path = require("path")

function build_slug(competition_short_name, season, round, index ){  
  return competition_short_name + season.replace('/','') + "-" + round.toLowerCase() + "-" + index.padStart(3, "0")
}

function build_slug_from_data_from_match(match_info){
  return build_slug(match_info.competition_short_name,match_info.season.replace("-20",""),team_mapping.find_round_short_name(match_info.round),match_info.index)
}

async function fetch_and_create_metadata(comp_id,date,home_team,away_team,asset_type) {  
  let match_element = {}
  match_element = await fetch_and_populate_metadata(match_element,comp_id,date,home_team,away_team,asset_type)  
  return match_element
}

async function fetch_and_populate_metadata(match_element,comp_id,date,home_team,away_team,title_type,slug_from_name) {
  let match_info = await info_getter.getDataForMatch(comp_id,date,team_mapping.adapt_if_needed(home_team),team_mapping.adapt_if_needed(away_team))  
  if (match_info == null) {
    // if here, match not found, let's try by using the other competition_id
    const alternate_comp_id = info_getter.get_alternate_competition_short_name(comp_id)
    if (alternate_comp_id != null) {
      match_info = await info_getter.getDataForMatch(alternate_comp_id,date,team_mapping.adapt_if_needed(home_team),team_mapping.adapt_if_needed(away_team))
      // if here, we found the match with the alternate competition id, otherwise an exception will be thrown
      if (slug_from_name != null) {
        // here we need to replace the comp_id with alternate_comp_id in the slug
        slug_from_name = slug_from_name.replace(comp_id,alternate_comp_id)
      }
      if (match_element.public?.name != null) {
        // here we need to replace the comp_id with alternate_comp_id in the slug
        match_element.public.name = match_element.public.name.replace(comp_id,alternate_comp_id)
      }
    }
    if (match_info == null) {
      throw new Error("Match not found for " + home_team + " v " + away_team + " on " + date + " in every competition");
    }
  }  
  // chl | chc
  match_info.competition_short_name = team_mapping.adapt_competition(match_info.competition_short_name)
  // 
  let slug = slug_from_name
  if (slug == null) {
    slug = build_slug_from_data_from_match(match_info)
  }
  // match_element.match_id = slug

  // extract short for season
  const season_short_form = match_info.season.replace("-20","")
  // make sure slug contains correct season
  if (!slug.includes(season_short_form)) {
    slug = slug.replace(/(ch[lp])-/,match_info.competition_short_name + season_short_form + "-")
  }
  
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
  // match_element.date = date

  let asset_metadata = match_element.public.asset_metadata
  let info = asset_metadata.info
  info.match_id = slug
  info.team_home_name = match_info.home_team
  // Make sure the home_team in name matches the one in info, if not adapt it
  if (home_team != match_info.home_team){
    const regex_home = new RegExp(" - " + away_team + " v")
    match_element.public.name = match_element.public.name.replace(regex_home," - " + match_info.home_team + " v")
  }
  // Make sure the away_team in name matches the one in info, if not adapt it
  if (away_team != match_info.away_team){
    const regex_home = new RegExp(" v " + away_team + " -")
    match_element.public.name = match_element.public.name.replace(regex_home," v " + match_info.away_team + " -")
  }

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
  info.opta_id = match_info.opta_id

  asset_metadata.ip_title_id = adapt_slug_to_title_type(slug,title_type)
  asset_metadata.slug = asset_metadata.ip_title_id
  asset_metadata.title_type = normalize_title_type(title_type)
  asset_metadata.asset_type = get_asset_type(asset_metadata.title_type)

  asset_metadata.display_title = compute_display_title(match_element,title_type)  
  match_element.public.description = asset_metadata.display_title

  // if slug is different from slug_from_name, we need to update the name
  if (slug_from_name != null && slug != slug_from_name) {
    match_element.public.name = match_element.public.name.replace(slug_from_name,slug)
  }
  return match_element  
}

function adapt_slug_to_title_type(slug,title_type){
  if (title_type == null || title_type.toLowerCase().includes("match")){
    return slug
  } else {
    return slug + "-" + normalize_title_type(title_type).toLowerCase().replace(" ","-")
  }
}

function compute_display_title(match_info,title_type){
  // EPCR Champions Cup - 2023-2024 - R1 - Glasgow Warriors v Northampton Saints
  if (title_type.toLowerCase() != "match" && !title_type.toLowerCase().includes("match_feed") ) {
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
  if (title_type_lower.includes("ob_ssm_dump")) {
    return "ISO 2"
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
exports.fetch_and_populate_metadata = fetch_and_populate_metadata
exports.get_competition_id = info_getter.get_competition_id
exports.adapt_if_needed = team_mapping.adapt_if_needed
exports.adapt_competition_short_name = info_getter.adapt_competition_short_name
exports.find_season_year = info_getter.find_season_year

// TO BE REMOVED - ONLY FOR TESTING
// fetch_and_create_metadata_from_s3("s3://epcrwbdarch/RGU_ECC_SAR_V_CAR_2022-04-17_OB_EVS_DUMP.mxf") 
// console.log("pippo")


