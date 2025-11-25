const fs = require('fs')
const path = require('path')
const assert = require('assert')
const cricket_australia_variants = require('../action_cricket_australia_variants')
const temp_data_folder = "/Users/andrea/temp/cricket_temp_data"

const providerMatchId = 65187
// Sample input payload
const fixture_inputs = require(`./event-${providerMatchId}.json`) // Save your payload as test-payload.json
const store_play_data_folder = temp_data_folder


const outputFile = path.join(store_play_data_folder, `event-${providerMatchId}.json`)

const translated_first_inning_file = path.join(store_play_data_folder, `translated-${providerMatchId}-first-inning.json`)
const translated_second_inning_file = path.join(store_play_data_folder, `translated-${providerMatchId}-second-inning.json`)
const translated_third_inning_file = path.join(store_play_data_folder, `translated-${providerMatchId}-third-inning.json`)

// Delete the output file before test to ensure clean state
if (fs.existsSync(outputFile)) {
  fs.unlinkSync(outputFile)
}

// Delete translated files before test to ensure clean state
if (fs.existsSync(translated_first_inning_file)) {
  fs.unlinkSync(translated_first_inning_file)
}
if (fs.existsSync(translated_second_inning_file)) {
  fs.unlinkSync(translated_second_inning_file)
}
if (fs.existsSync(translated_third_inning_file)) {
  fs.unlinkSync(translated_third_inning_file)
}

console.log('🧪 Running test: Cricket Processor')
const params = { client: "test-client", baseFolder: "/tmp" }

try {
  
  fixture_inputs.web_hooks.forEach((webhook) => {
    inputs = {"web_hooks": webhook, "store_play_data_folder": store_play_data_folder}
    outputs = {}
    const params = { client: "test-client", baseFolder: "/tmp" }

    const my_processor = new cricket_australia_variants(params)
    my_processor.executeStorePlayData({"inputs" : inputs, "outputs" : outputs})    

    // Simple shape test
    assert(Object.keys(outputs) != null, 'Missing outputs')
    assert(outputs.event_file_path === outputFile, `Expected event_file_path to be ${outputFile}, found ${outputs.event_file_path}`)    
    if (webhook.event.type != "scoreUpdate") {
      assert(outputs.transalted_event_file_path != null, 'Missing translated event file path')
    }
    
  })


  // After all events processed, read file and validate
  
  const converted_first_inning_events = JSON.parse(fs.readFileSync(translated_first_inning_file, 'utf-8'))
  const converted_second_inning_events = JSON.parse(fs.readFileSync(translated_second_inning_file, 'utf-8'))
  const converted_third_inning_events = JSON.parse(fs.readFileSync(translated_third_inning_file, 'utf-8'))
  const original_events = JSON.parse(fs.readFileSync(outputFile, 'utf-8'))
  
  const fileContent = fs.readFileSync(outputFile, 'utf-8')
  const events = JSON.parse(fileContent)

  console.log(`✅ File written: ${outputFile}`)

  assert(converted_first_inning_events.metadata_tags.game_events_all__first_innings != null, `Expected first inning metadata tag, found ${Object.keys(converted_first_inning_events.metadata_tags)}`)
  assert(converted_second_inning_events.metadata_tags.game_events_all__second_innings != null, `Expected second inning metadata tag, found ${Object.keys(converted_second_inning_events.metadata_tags)}`)
  assert(converted_third_inning_events.metadata_tags.game_events_all__third_innings != null, `Expected third inning metadata tag, found ${Object.keys(converted_third_inning_events.metadata_tags)}`)
  
  const total_events_saved = converted_first_inning_events.metadata_tags.game_events_all__first_innings.tags.length +
    converted_second_inning_events.metadata_tags.game_events_all__second_innings.tags.length +
    converted_third_inning_events.metadata_tags.game_events_all__third_innings.tags.length
  
  console.log(`✅ Total events saved: ${total_events_saved}`)
    
  let expected_events = 0
  for (original_event of original_events) {
    if (original_event.event.type !== "scoreUpdate") {
      expected_events += 1
    }
  } 

  // original_events has some scoreChange events that are not converted
  assert(total_events_saved == expected_events, 
    `Expected ${expected_events} events, found ${total_events_saved}`)

  console.log('🎉 All tests passed!')
} catch (err) {
  console.error('❌ Test failed:', err.message)
  process.exit(1)
}
