// the kafka instance and configuration variables are the same as before
// import the `Kafka` instance from the kafkajs library
const { Kafka } = require("kafkajs")


const clientId = "my-app" // the client ID lets kafka know who's producing the messages
const brokers = ["kafka:9092"] // we can define the list of brokers in the cluster
const topic = "message-log" // this is the topic to which we want to write messages
const kafka = new Kafka({ clientId, brokers }) // initialize a new kafka client and initialize a producer from it

// create a new consumer from the kafka client, and set its group ID
// the group ID helps Kafka keep track of the messages that this client
// is yet to receive
const consumer = kafka.consumer({ groupId: clientId })

const consume = async () => {
  // first, we wait for the client to connect and subscribe to the given topic
  await consumer.connect()
  await consumer.subscribe({ topic })
  await consumer.run({
    // this function is called every time the consumer gets a new message
    eachMessage: ({ message }) => {
      // here, we just log the message to the standard output
      console.log(`received message: ${message.value}`)
    },
  })
}

module.exports = consume