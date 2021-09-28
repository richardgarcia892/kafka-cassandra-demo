// import the `Kafka` instance from the kafkajs library
const { Kafka } = require("kafkajs")


const clientId = "my-app" // the client ID lets kafka know who's producing the messages
const brokers = ["kafka:9092"] // we can define the list of brokers in the cluster
const topic = "message-log" // this is the topic to which we want to write messages

// initialize a new kafka client and initialize a producer from it
const kafka = new Kafka({ clientId, brokers })
const producer = kafka.producer()

// we define an async function that writes a new message each second
const produce = async () => {
  await producer.connect()
  let i = 0

  // after the produce has connected, we start an interval timer
  setInterval(async () => {
    const message = messageGenerator()
    try {
      // send a message to the configured topic with
      // the key and value formed from the current value of `i`
      await producer.send({
        topic,
        messages: [
          {
            key: String(i),
            value: JSON.stringify(message)
          }
        ]
      })

      // if the message is written successfully, log it and increment `i`
      console.log("writes: ", i)
      i++
    } catch (err) {
      console.error("could not write message " + err)
    }
  }, 1000)
}

function messageGenerator() {
  const items = ['Bad', 'Good', 'Average', 'Excellent'];
  const quality = items[Math.floor(Math.random() * items.length)];
  const timestamp = Math.round(new Date().getTime() / 1000);
  value = Math.round(Math.random() * (20 - 15) + 15);
  return {
    access_path: topic,
    quality,
    timestamp,
    value
  }
}

module.exports = produce