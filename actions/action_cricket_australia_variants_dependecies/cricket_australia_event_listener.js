const fs = require('fs')
const path = require('path')

function createCricketProcessor(payload, base_folder = __dirname) {
  
  const providerMatchId = extractProviderMatchId(payload)

  let video_start_absolute_timestamp = null

  function set_start_time_ts(start_absolute_timestamp) {
    if (start_absolute_timestamp) {
      video_start_absolute_timestamp = start_absolute_timestamp
    } else {
      throw new Error('Invalid video start absolute timestamp')
    }
  }

  function extractProviderMatchId(payload) {
    if (
      payload &&
      payload.type === 'cricket' &&
      payload.event &&
      typeof payload.event.providerMatchId === 'string'
    ) {
      return payload.event.providerMatchId
    }
    throw new Error('Invalid or missing providerMatchId in the payload')
  }

  

  function transformEvent(event) {
    const {
      type,
      ballTimestamp = null,
      inningsNumber,
      overNumber,
      ballNumber,
      battingTeamName = 'Unknown Team',
      bowlingTeamName = 'Unknown Team',
      shotType = null,
      fieldPosition = null
    } = event

    const startTime = ballTimestamp || null
    const endTime = ballTimestamp || null

    let description = ''
    let event_type = ''
        
    const event_id = event.providerMatchId + event.ballTimestamp   
    const filename = path.join(base_folder, `${providerMatchId}-${stringify_inning(inningsNumber)}-inning.json`) 

    // Load events from file if it exists
    const existing_tags = load_from_file(filename)

    switch (type) {
      case 'appeal':
        description = `Appeal - ${event.appealType} appeal by [${event.fielderName || 'Unknown'}] <${bowlingTeamName}>`
        event_type = 'Appeal'
        break

      case 'halfCentury':
        description = `Half-century - [${event.batterName || 'Batter'}] <${battingTeamName}> reaches ${event.batterRunsTotal} runs.`
        event_type = 'Half-century'
        break

      case 'century':
        description = `Century -  [${event.batterName || 'Batter'}] <${battingTeamName}> reaches ${event.batterRunsTotal} runs.`
        event_type = 'Century'
        break

      case 'dismissal':
        description = `Dismissal - ${event.dismissalType} by [${event.fielderName || 'Unknown'}] <${bowlingTeamName}>`
        event_type = 'Dismissal'
        break

      case 'droppedCatch':
        description = `Dropped-Catch - [${event.fielderName || 'Unknown'}] <${bowlingTeamName}> at ${event.fieldPosition || 'Unknown Position'}`
        event_type = 'Dropped-Catch'
        break

      case 'boundary':
        description = `Boundary - [${event.batterName || 'Batter'}] <${battingTeamName}> hits a ${event.batterRunsScored} with a ${event.shotType || 'Unknown'} shot in ${event.fieldPosition || 'Unknown Position' }`
        event_type = 'Boundary'
        break

      case 'firstBallInnings':
        description = `First-Ball Inning ${event.inningsNumber}`
        event_type = 'First-Ball Inning'
        break

      default:
        description = `Unknown event type: ${type}`
    }

    const transformed = {
      absolute_start_timestamp: startTime,
      absolute_end_timestamp: endTime,
      start_time: convert_to_relative(startTime),
      end_time: convert_to_relative(endTime),
      text: [
        `${event_type}: ${description}`,
        `I:${inningsNumber || '-'} O:${overNumber || '-'} Ball_Number:${ballNumber || '-'}`,
        `id: ${event_id}`
      ]
    }

    const newEventStr = JSON.stringify(transformed)

    // Check if exact string already exists in the list
    const existingStrings = existing_tags.map(e => JSON.stringify(e));
    if (existingStrings.includes(newEventStr)) {  
      // if it exists, return null  
      return null
    }

    // Add the new tag and save back to file
    const updatedTags = [...existing_tags, transformed]

    // const inningsKey = `game_events_all__${stringify_inning(inningsNumber)}_innings`;
    const inningsKey = `game_events_all__${stringify_inning(inningsNumber)}_half`;

    const outputJson = {
      version: 1,
      video_level_tags: {},
      metadata_tags: {
        [inningsKey]: {
          label: `Event - ALL: ${stringify_inning(inningsNumber,true)} Half`,
          tags: updatedTags
        }
      }
    }
    
    // Save to file if providerMatchId is active
    if (filename) {
      
      try {
        fs.writeFileSync(filename, JSON.stringify(outputJson, null, 2), 'utf-8')
      } catch (err) {
        console.error(`Failed to write to ${filename}:`, err.message)
      }
    }

    return transformed
  }

  return {
    extractProviderMatchId,
    transformEvent,
    set_start_time_ts
  }

  /**
   * Converts an absolute timestamp to a relative one based on the video start timestamp.
   * @param {*} timestamp 
   * @returns the relative timestamp in milliseconds or -1 if video_start_absolute_timestamp is not set
   */
  function convert_to_relative(timestamp) {
    if (video_start_absolute_timestamp) {
      return (timestamp - video_start_absolute_timestamp)*1000 // Convert to milliseconds
    } else {
      return -1
    }
  }

  // There are only 4 innings in cricket, so we can use a simple array to map them
  function stringify_inning(inningsNumber,capitalize = false) {
    if (capitalize) {
      return ["First", "Second", "Third", "Fourth"][inningsNumber - 1] || "Unknown"
    }else {
      return ["first", "second", "third", "fourth"][inningsNumber - 1] || "unknown"
    }
  }


  function load_from_file(filename) {
    const existing_tags = []
    if (fs.existsSync(filename)) {
      try {
        const fileContent = fs.readFileSync(filename, 'utf-8')
        const parsed = JSON.parse(fileContent)
        if (parsed &&
          parsed.metadata_tags &&
          typeof parsed.metadata_tags === 'object') {
          // Load existing tags array from the nested structure
          const inningsKey = Object.keys(parsed.metadata_tags)[0]
          if (parsed.metadata_tags[inningsKey] &&
            Array.isArray(parsed.metadata_tags[inningsKey].tags)) {
            existing_tags.push(...parsed.metadata_tags[inningsKey].tags)
          }
        } else {
          throw new Error(`Invalid prologue format in file: ${filename}`)
        }
      } catch (err) {
        throw new Error(`Failed to load or parse file for providerMatchId "${providerMatchId}": ${err.message}`)
      }
    }
    return existing_tags
  }
}

module.exports = createCricketProcessor
// Usage example
// const cricketProcessor = createCricketProcessor('some_provider_match_id')