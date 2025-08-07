const fs = require('fs')
const path = require('path')
const assert = require('assert')
const createCricketProcessor = require('./cricket_australia_event_listener')

// Sample input payload
const input = require('./test-payload.json') // Save your payload as test-payload.json

const providerMatchId = '64389'
const outputFile = path.join(__dirname, `${providerMatchId}.json`)

// Delete the output file before test to ensure clean state
if (fs.existsSync(outputFile)) {
  fs.unlinkSync(outputFile)
}

console.log('🧪 Running test: Cricket Processor')

try {
  input.webhooks.forEach((webhook) => {
    const payload = { ...webhook } // Individual webhook treated as payload
    const processor = createCricketProcessor(payload)
    const transformed = processor.transformEvent(payload.event)

    // Simple shape test
    assert(transformed.start_time != null, 'Missing start_time')
    assert(Array.isArray(transformed.text), 'text should be an array')
    // assert(transformed.text[0].includes('Boundary') | transformed.text[0].includes('Appeal'), 'Invalid text line format')
  })

  // After all events processed, read file and validate
  const fileContent = fs.readFileSync(outputFile, 'utf-8')
  const events = JSON.parse(fileContent)

  console.log(`✅ File written: ${outputFile}`)
  console.log(`✅ Total events saved: ${events.length}`)
  assert(events.length === input.webhooks.length, `Expected ${input.webhooks.length} events, found ${events.length}`)

  console.log('🎉 All tests passed!')
} catch (err) {
  console.error('❌ Test failed:', err.message)
  process.exit(1)
}
