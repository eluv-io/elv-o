const fs = require('fs')
const { get } = require('http')
const path = require('path')

function createCricketProcessor(payload, base_folder = __dirname) {
  const providerMatchId = extractProviderMatchId(payload)
  const filename = path.join(base_folder, `${providerMatchId}.json`)
  const events = []


  // Load events from file if it exists
  if (fs.existsSync(filename)) {
    try {
      const fileContent = fs.readFileSync(filename, 'utf-8')
      const parsed = JSON.parse(fileContent)
      if (Array.isArray(parsed)) {
        events.push(...parsed)
      } else {
        throw new Error(`Invalid JSON format in file: ${filename}`)
      }
    } catch (err) {
      throw new Error(`Failed to load or parse file for providerMatchId "${providerMatchId}": ${err.message}`)
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
      ballNumber
    } = event

    const startTime = ballTimestamp || null
    const endTime = ballTimestamp || null

    let description = ''
    let event_type = ''
    
    const teamName = '' // This information is not provided in the payload
    const event_id = event.providerMatchId + event.ballTimestamp

    switch (type) {
      case 'appeal':
        description = `Appeal - ${event.appealType} appeal by [${event.fielderName || 'Unknown'}] ${teamName}`
        event_type = 'Appeal'
        break

      case 'halfCentury':
        description = `Half-century - [${event.batterName || 'Batter'}] ${teamName} reaches ${event.batterRunsTotal} runs.`
        event_type = 'Half-century'
        break

      case 'century':
        description = `Century -  [${event.batterName || 'Batter'}] ${teamName} reaches ${event.batterRunsTotal} runs.`
        event_type = 'Century'
        break

      case 'dismissal':
        description = `Dismissal - ${event.dismissalType} by [${event.fielderName || 'Unknown'}] ${teamName}`
        event_type = 'Dismissal'
        break

      case 'droppedCatch':
        description = `Dropped-Catch - [${event.fielderName || 'Unknown'}] ${teamName} at ${event.fieldPosition || 'Unknown Position'}`
        event_type = 'Dropped-Catch'
        break

      case 'boundary':
        description = `Boundary - [${event.batterName || 'Batter'}] ${teamName} hits a ${event.batterRunsScored}`
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
      start_time: startTime,
      end_time: endTime,
      text: [
        `${event_type}: ${description}`,
        `I:${inningsNumber || '-'} O:${overNumber || '-'} Ball_Number:${ballNumber || '-'}`,
        `id: ${event_id}`
      ]
    }

    const newEventStr = JSON.stringify(transformed)

    // Check if exact string already exists in the list
    const existingStrings = events.map(e => JSON.stringify(e));
    if (existingStrings.includes(newEventStr)) {  
      // if it exists, return the event without saving    
      return transformed
    }

    // Save to file if providerMatchId is active
    if (filename) {
      events.push(transformed)
      try {
        fs.writeFileSync(filename, JSON.stringify(events, null, 2), 'utf-8')
      } catch (err) {
        console.error(`Failed to write to ${filename}:`, err.message)
      }
    }

    return transformed
  }

  return {
    extractProviderMatchId,
    transformEvent
  }
}

module.exports = createCricketProcessor
// Usage example
// const cricketProcessor = createCricketProcessor('some_provider_match_id')