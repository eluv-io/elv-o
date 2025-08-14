const team_epcr_map = new Map([
  ["ASM Clermont Auvergne","CLE"],
  ["Bath Rugby","BAT"],
  ["Benetton Rugby","BEN"],
  ["Bristol Bears","BRS"],
  ["Castres Olympique","CAS"],
  ["DHL Stormers","STO"],
  ["Exeter Chiefs","EXE"],
  ["Glasgow Warriors","GLA"],
  ["Harlequins","HAR"],
  ["Hollywoodbets Sharks","SHA"], // Durban Sharks
  ["Leicester Tigers","LEIC"], //LEI
  ["Leinster Rugby","LEIN"], //LSR
  ["Munster Rugby","MUN"],
  ["Northampton Saints","NOR"],
  ["Racing 92","R92"],
  ["RC Toulon","TLN"],
  ["Sale Sharks","SAL"],
  ["Saracens","SAR"],
  ["Stade Francais Paris","STA"],
  ["Stade Rochelais","LAR"],
  ["Stade Toulousain","TLS"],
  ["Ulster Rugby","ULS"],
  ["Union Bordeaux-Begles","BOR"],
  ["Vodacom Bulls","BUL"],
  ["Avrion Bayonnais","BAY"],
  ["Black Lion","BLA"],
  ["Cardiff Rugby","CAR"],
  ["Connacht Rugby","CON"],
  ["Dragons RFC","DRA"],
  ["Edinburgh Rugby","EDI"],
  ["Emirates Lions","LIO"],
  ["Gloucester Rugby","GLO"],
  // ["Lyon Olympique Universitaire Rugby (LOU Rugby)","LYN"],
  ["Lyon","LYN"],
  ["Montpellier Herault Rugby","MON"],  
  ["Newcastle Falcons","NEW"],
  ["Ospreys","OSP"],
  ["RC Vannes","VAN"],
  ["Scarlets","SCA"],
  ["Section Paloise","PAU"],
  ["Toyota Cheetahs USAP","CHE"],
  ["USAP","PER"],
  ["Zebre Parma","ZEB"],
  ["FC Grenoble Rugby","GRE"],
  ["CA Brive","BRI"],  
  ["Enisei-STM","ENI"],  
  ["Worcester Warriors","WOR"],
  ["SCM Rugby Timișoara","TIM"],
  ["Wasps RFC","WAS"],
  ["SU Agen","AGE"],
  ["Krasny Yar","KRA"],
  ["London Irish","LIR"],
  ["Biarritz Olympique","BIA"],
  ["Romanian Wolves","WOL"],
  ["London Welsh","LWE"],
  ["Rugby Rovigo Delta","ROV"],
  ["Rugby Calvisano","CAM"],
  ["Oyonnax Rugby","OYO"] //
])

const team_code_to_epcr_code = new Map([
  ["BRI","BRS"],
  ["TON","TLN"],
  ["NGD","DRA"],
  ["MOP","MON"],
  ["BRV","BRI"],
  ["TON","TLN"],
  ["TOS","TLS"],
  ["WEL","LWE"],
  ["TRE","BEN"],
  ["LSR","LEIN"],
  ["LEI","LEIC"],
  ["LYO","LYN"],
  ["PGN","PER"],
  ["RAC","R92"]
])


const team_origin_to_epcr = new Map([
  ["Stade Français Paris","Stade Francais Paris"],
  ["Grenoble","FC Grenoble Rugby"],
  // ["Lyon O.U.","Lyon Olympique Universitaire Rugby (LOU Rugby)"],
  ["Lyon O.U.","Lyon"],
  ["Brive","CA Brive"],
  ["Enisei-STM","Enisei-STM"],
  ["Worcester Warriors","Worcester Warriors"],
  ["Timisoara Saracens","SCM Rugby Timișoara"],
  ["La Rochelle","Stade Rochelais"],
  ["Bayonne","Avrion Bayonnais"],
  ["Pau","Section Paloise"],  
  ["Wasps","Wasps RFC"],
  ["Toulon","RC Toulon"],
  ["Montpellier","Montpellier Herault Rugby"],
  ["Toulouse","Stade Toulousain"],
  ["Bordeaux-Begles","Union Bordeaux-Begles"],
  ["Clermont Auvergne","ASM Clermont Auvergne"],
  ["Agen","SU Agen"],
  ["Krasny Yar","Krasny Yar"],
  ["London Irish","London Irish"],
  ["Oyonnax","Oyonnax Rugby"],
  ["Perpignan","USAP"],
  ["Transvecta Calvisano","Rugby Calvisano"],
  ["Biarritz Olympique","Biarritz Olympique"],
  ["Toyota Cheetahs","Toyota Cheetahs USAP"],
  ["Vannes","RC Vannes"],
  ["Lions","Emirates Lions"],
  ["Newcastle Red Bulls","Newcastle Falcons"]
  ])


const similar_name_mapping = new Map([
  // ["Lyon","Lyon Olympique Universitaire Rugby (LOU Rugby)"],
  ["Aviron Bayonnais","Avrion Bayonnais"],
  ["Bath","Bath Rugby"],
  ["Bristol Rugby","Bristol Bears"],
  ["Edinburgh Rugby [Interlaced]","Edinburgh Rugby"],
  ["Durban Sharks","Hollywoodbets Sharks"],
  ["Durban Sharks - Deint01","Hollywoodbets Sharks"],
  ["Durban Sharks - Deint02","Hollywoodbets Sharks"],
  ["Gloucester","Gloucester Rugby"],
  ["Cell C Sharks","Hollywoodbets Sharks"],
  ["Connact Rugby","Connacht Rugby"],
  ["Exeter Rugby","Exeter Chiefs"],
  ["Exter Rugby","Exeter Chiefs"],  
  ["Exter","Exeter Chiefs"],
  ["Exeter","Exeter Chiefs"],    
  ["Clermont","ASM Clermont Auvergne"],
  ["Cardiff Blues","Cardiff Rugby"],
  ["Cardiff Rubgy","Cardiff Rugby"],
  ["Castre Olympique","Castres Olympique"],
  ["Castres","Castres Olympique"],  
  ["Dragons (dirty)","Dragons RFC"],
  ["Glasgow","Glasgow Warriors"],
  ["Leinster","Leinster Rugby"],
  ["Leicester Rugby","Leicester Tigers"],
  ["Leicetser Tigers","Leicester Tigers"],    
  ["Newscatle Falcons","Newcastle Falcons"], 
  ["Racing","Racing 92"],
  ["WASPS","Wasps RFC"],    
  ["Wasps","Wasps RFC"],    
  ["Black Lions","Black Lion"],  
  ["Toyota Cheetahs","Toyota Cheetahs USAP"],
  ["Toyota Cheetahs","Toyota Cheetahs USAP"],  
  ["Sections Paloise","Section Paloise"],
  ["UBB","Union Bordeaux-Begles"],  
  ["Union Bordeaux-Bègles","Union Bordeaux-Begles"],  
  ["Bordeaux-Bègles","Union Bordeaux-Begles"],  
  ["Montpellier Hérault Rugby","Montpellier Herault Rugby"],
  ["Montpellier HR","Montpellier Herault Rugby"],
  ["Stade Rochleais","Stade Rochelais"],
  ["Bristol","Bristol Bears"],
  ["Bristol Bears VoD","Bristol Bears"],
  ["Munster","Munster Rugby"],
  ["Racing92","Racing 92"],
  ["LaRochelle","Stade Rochelais"],
  ["La Rochelle","Stade Rochelais"],
  ["Stade Francais","Stade Francais Paris"],
  ["Stade Français Paris Paris","Stade Francais Paris"],
  ["Sale","Sale Sharks"],
  ["Munster","Munster Rugby"],
  ["Harelquins","Harlequins"],
  ["Ulster","Ulster Rugby"],
  ["Leicester","Leicester Tigers"],
  ["Newcastle","Newcastle Falcons"],
  ["Benetton","Benetton Rugby"],
  ["Connacht","Connacht Rugby"],
  ["Cardiff","Cardiff Rugby"],
  ["Northampton","Northampton Saints"],
  ["Edinburgh","Edinburgh Rugby"],
  ["Biarritz","Biarritz Olympique"],
  ["Zebre","Zebre Parma"],
  ["Zebra Rugby","Zebre Parma"],
  ["Bordeaux","Union Bordeaux-Begles"],
  ["Worcester","Worcester Warriors"],
  ["Union Bordeaux Begles","Union Bordeaux-Begles"],
  ["Union Bordeaux Bègles","Union Bordeaux-Begles"],
  ["Bordeaux Begles","Union Bordeaux-Begles"],
  ["Worcester Warrior","Worcester Warriors"],
  ["- Ospreys","Ospreys"],
  ["The Sharks","Hollywoodbets Sharks"],
  ["NG Dragons","Dragons RFC"],
  ["Dragons","Dragons RFC"],
  ["Brive","CA Brive"],
  ["Newcastle Red Bulls","Newcastle Falcons"]
])


function adapt_competition(c_code){
  switch (c_code) {
    case "HCC","hcc":
      return "chc";
    case "ECC","ecc":
      return "chp";
  }
  return c_code
}
/**
 * 
 * @param {*} team_name 
 * @returns the adapted team name (or the original team_name if adaption is not needed or not possible) 
 */
function adapt_if_needed(team_name){
  let adapted_name = similar_name_mapping.get(team_name);
  if (adapted_name != null){
    return adapted_name;
  }
  adapted_name = team_origin_to_epcr.get(team_name);
  if (adapted_name != null){
    return adapted_name;
  }
  return team_name;
}

function find_epcr_team_name(original_name){
  let result = team_origin_to_epcr.get(original_name)
  if (result == null){
    // make sure orginal name is in epcr map
    if (team_epcr_map.get(original_name) != null) {
      return original_name
    }
    throw new Error(original_name + " not recognized as a team");    
  }
  return result
}

function find_epcr_team_code(team_name){
  let code = team_epcr_map.get(team_name)
  if (code == null){    
    code = team_epcr_map.get(find_epcr_team_name(team_name))
  }
  if (code == null){
    throw new Error(team_name + " not recognized as a team");    
  }
  return code
}

function find_epcr_team_code_from_s3_code(s3_team_code){
  return team_code_to_epcr_code.get(s3_team_code) || s3_team_code;
}

function find_epcr_team_from_code(team_code){
  for (const [key, value] of team_epcr_map) {
    if (value == team_code) {
      return key
    }
  }
  throw new Error(team_code + " not recognized as a team code");    
}

function find_competition_name(competition_code){
  switch (competition_code) {
    case "chl":
      return "EPCR Challenge Cup";
    case "chp":
      return "EPCR Champions Cup";
  }
}

function find_round_name(round_short_form){
  const regEx = new RegExp(/R(\d)$/)
  if (round_short_form.match(regEx) != null)
    return "Group Stage Round " + round_short_form.match(regEx)[1];
  switch (round_short_form.toUpperCase()) {
    case "R16":
    case "RO16":  
    case "RNULL":
      return "Round of 16";
    case "TF":
    case "F":
      return "Final";
    case "SF":
      return "Semifinals";
    case "QF":
      return "Quarterfinals";    
    case "PO":
      return "Playoff";
    case "KO":
      return "Knockout";      
    default:
      throw new Error("Can't find round long form for " + round_short_form);
  }
}

function find_round_short_name(original_round) {
  switch(original_round.toUpperCase()) {
    case "TF":
      return "F"
    case "RNULL":
      return "RO16"
    case "R6":
      return "QF"
    case "R7":
      return "SF"
    case "R8":
      return "F"
    case "PLAYOFF":
      return "PO"
    case "KNOCKOUT":
      return "KO"
  }

  return original_round
}


exports.find_epcr_team_name = find_epcr_team_name
exports.find_epcr_team_code = find_epcr_team_code
exports.find_epcr_team_from_code = find_epcr_team_from_code
exports.adapt_if_needed = adapt_if_needed
exports.find_competition_name = find_competition_name
exports.find_round_name = find_round_name
exports.find_round_short_name = find_round_short_name
exports.adapt_competition = adapt_competition
exports.find_epcr_team_code_from_s3_code = find_epcr_team_code_from_s3_code
