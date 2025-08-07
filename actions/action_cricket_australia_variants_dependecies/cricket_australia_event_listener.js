const fs = require('fs')
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
    
    const teamName = '' // This information is not provided in the payload
    const event_id = event.providerMatchId + event.ballTimestamp

    switch (type) {
      case 'appeal':
        description = `Appeal - ${event.appealType} appeal by [${event.fielderName || 'Unknown'}] ${teamName}`
        break

      case 'halfCentury':
        description = `Half-century - [${event.batterName || 'Batter'}] ${teamName} reaches ${event.batterRunsTotal} runs.`
        break

      case 'century':
        description = `Century -  [${event.batterName || 'Batter'}] ${teamName} reaches ${event.batterRunsTotal} runs.`
        break

      case 'dismissal':
        description = `Dismissal - ${event.dismissalType} by [${event.fielderName || 'Unknown'}] ${teamName}`
        break

      case 'droppedCatch':
        description = `Dropped Catch by [${event.fielderName || 'Unknown'}] ${teamName} at ${event.fieldPosition || 'Unknown Position'}`
        break

      case 'boundary':
        description = `Boundary - [${event.batterName || 'Batter'}] ${teamName} hits a ${event.batterRunsScored}`
        break

      case 'firstBallInnings':
        description = `Start of innings ${event.inningsNumber}`
        break

      default:
        description = `Unknown event type: ${type}`
    }

    const transformed = {
      start_time: startTime,
      end_time: endTime,
      text: [
        `Event - ${type}: ${description}`,
        `I:${inningsNumber || '-'} O:${overNumber || '-'} B:${ballNumber || '-'}`,
        `id: ${event_id}`
      ]
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