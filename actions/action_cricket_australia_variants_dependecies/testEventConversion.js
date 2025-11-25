const fs = require('fs')
const path = require('path')
const assert = require('assert')
const cricket_australia_variants = require('../action_cricket_australia_variants')


const inputs = {
  event_file_path: path.join(__dirname, 'event-65187.json'), // Path to the input file
  store_play_data_folder: "/Users/andrea/temp/cricket_temp_data"
}

let outputs = {}

console.log('🧪 Running test: Cricket Event Conversion')

try {
  const params = { client: "test-client", baseFolder: "/tmp" }
  let my_processor = new cricket_australia_variants(params)
  my_processor.executeConvertPlayData({"inputs" : inputs, "outputs" : outputs})

  const outputFiles = outputs.transalted_event_file_path
  console.log(`✅ File written: ${outputFiles}`)

  // After all events processed, read file and validate
  const converted_first_inning_events = JSON.parse(fs.readFileSync(outputFiles[0], 'utf-8'))
  const converted_second_inning_events = JSON.parse(fs.readFileSync(outputFiles[1], 'utf-8'))
  const converted_third_inning_events = JSON.parse(fs.readFileSync(outputFiles[2], 'utf-8'))
  const original_events = JSON.parse(fs.readFileSync(outputs.event_file_path, 'utf-8'))

  assert(converted_first_inning_events.metadata_tags.game_events_all__first_innings != null, `Expected first innings metadata tag, found ${Object.keys(converted_first_inning_events.metadata_tags)}`)
  assert(converted_second_inning_events.metadata_tags.game_events_all__second_innings != null, `Expected second innings metadata tag, found ${Object.keys(converted_second_inning_events.metadata_tags)}`)
  assert(converted_third_inning_events.metadata_tags.game_events_all__third_innings != null, `Expected third innings metadata tag, found ${Object.keys(converted_third_inning_events.metadata_tags)}`)

  const total_events_saved = converted_first_inning_events.metadata_tags.game_events_all__first_innings.tags.length +
    converted_second_inning_events.metadata_tags.game_events_all__second_innings.tags.length +
    converted_third_inning_events.metadata_tags.game_events_all__third_innings.tags.length

  console.log(`✅ Total events saved: ${total_events_saved}`)
  
  let expected_events = 0
  const counted_event = []
  for (original_event of original_events.web_hooks? original_events.web_hooks : original_events) {
    if (original_event.event.type !== "scoreUpdate") {      
      if (!counted_event.includes(JSON.stringify(original_event))) {
        expected_events += 1
        counted_event.push(JSON.stringify(original_event))
      }
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
