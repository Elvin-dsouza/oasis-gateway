var express = require('express');
var router = express.Router();
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'oasis-sensor-log',
  brokers: [`acesd.online:29092`]
});

const kafkaLog = new Kafka({
  clientId: 'oasis-sensor-log',
  brokers: [`acesd.online:29093`]
});

const logHistoricalStream = async (pm25, pm10, pm1, temperature, humidity) => {
  const producerLog = kafkaLog.producer();
  await producerLog.connect();
  await producerLog.send({
    topic: 'sensor-output-log-hist',
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
  await producerLog.disconnect();
};

let lastLog = 0;
const logSensorInformation = async (pm25, pm10, pm1, temperature, humidity) => {
  const producer = kafka.producer();
  
  
  await producer.connect();
  
  if(!lastLog) {
    lastLog = Date.now();
    await logHistoricalStream(pm25, pm10, pm1, temperature, humidity);
  }
  if(Date.now() >= (lastLog + (60 * 1000 * 1))){
    lastLog = Date.now();
    await logHistoricalStream(pm25, pm10, pm1, temperature, humidity);
  }
  
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
  await producer.disconnect();
};

/* GET users listing. */
router.get('/', function(req, res, next) {
  logSensorInformation(
    req.query.pm25, 
    req.query.pm10, 
    req.query.pm1,
    req.query.temperature,
    req.query.humidity
  );
  res.send('OK');
});

module.exports = router;