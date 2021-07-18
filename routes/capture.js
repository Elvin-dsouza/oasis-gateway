var express = require('express');
var router = express.Router();
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'oasis-sensor-log',
  brokers: [`acesd.online:29092`]
});


let lastLog = 0;
/**
 * temporarily store average values between db inserts
 */
 const currentAverage = {
  pm25: 0.0,
  pm10: 0.0,
  pm1: 0.0,
  temperature: 0.0,
  humidity: 0.0,
};
let numRecordsSinceLastInsert = 0;

const logCompactedStream = async (pm25, pm10, pm1, temperature, humidity) => {
  const output = {
    pm25, pm10, pm1, temperature, humidity
  };
  if(!lastLog) {
      lastLog = Date.now();
      numRecordsSinceLastInsert ++;
      Object.keys(currentAverage).forEach((key) => {
          currentAverage[key] += parseInt(output[key], 10);
      });
      // Calulate Average
      Object.keys(currentAverage)
          .forEach(key => (currentAverage[key] = currentAverage[key]/numRecordsSinceLastInsert));
      // Store data
      await logSensorInformation(currentAverage);
      // Reset State
      Object.keys(currentAverage)
          .forEach((key) => (currentAverage[key] = 0.0));
      numRecordsSinceLastInsert = 0;
  }
  else if(Date.now() >= (lastLog + (5 * 1000))){
      lastLog = Date.now();
       // Calulate Average
       Object.keys(currentAverage)
       .forEach(key => (currentAverage[key] = currentAverage[key]/numRecordsSinceLastInsert));
      // Store data
      await logSensorInformation(currentAverage);
      // Reset State
      Object.keys(currentAverage)
          .forEach((key) => (currentAverage[key] = 0.0));
      numRecordsSinceLastInsert = 0;
  }
  else {
      numRecordsSinceLastInsert ++;
      Object.keys(currentAverage).forEach((key) => {
          currentAverage[key] += parseInt(output[key], 10);
      });
  }
};


const logSensorInformation = async ({pm25, pm10, pm1, temperature, humidity}) => {
  const producer = kafka.producer();
  await producer.connect();
  
  await producer.send({
    topic: 'sensor-output',
    messages: [
      { value: JSON.stringify({ 
        pm25,
        pm10,
        pm1,
        temperature,
        humidity,
        timestamp: Date.now()
      }) },
    ],
  });
  console.log("COMPACTION: send consolidated event");
  await producer.disconnect();
};

/* GET users listing. */
router.get('/', function(req, res, next) {
  logCompactedStream(
    req.query.pm25, 
    req.query.pm10, 
    req.query.pm1,
    req.query.temperature,
    req.query.humidity
  );
  res.send('OK');
});

module.exports = router;