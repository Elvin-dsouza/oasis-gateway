#!/usr/bin/env node
const { Kafka, logLevel } = require('kafkajs')
const chalk = require('chalk');

const kafka = new Kafka({
  clientId: 'oasis-sensor-log',
  logLevel:logLevel.NOTHING,
  brokers: ['acesd.online:29092']
});

const waitForMessages = async () => {
    const consumer = kafka.consumer({ groupId: 'sensor-output' })
    await consumer.connect()
    await consumer.subscribe({ topic: 'sensor-output', fromBeginning: false })
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            let output = JSON.parse(await message.value.toString());
            console.clear();
            output["latency"] = Date.now() - output.timestamp + " ms";
            output.timestamp = new Date(output.timestamp).toLocaleString();
            output.temperature = { celcius:`${output.temperature} *C`, farenheit: `${output.temperature * 1.8 + 32} *F` }; 
            console.table(output);
            // console.log(`Temperature ${output.temperature} *C  | Humidity ${output.humidity} % | Latency ${Date.now() - output.timestamp} ms`);
        },
    })
};

waitForMessages();
