var express = require('express');
var router = express.Router();
const { Kafka } = require('kafkajs');
var loki = require('lokijs');
var db = new loki('Example');
const kafka = new Kafka({
  clientId: 'oasis-sensor-log',
  brokers: [`acesd.online:29092`]
});

let lastLog = 0;
var log = db.addCollection('sensorLog', { indices: ['timestamp'] });
const logSensorInformation = async (pm25, pm10, pm1, temperature, humidity) => {
  const producer = kafka.producer()
  await producer.connect();
  if(!lastLog) {
    lastLog = Date.now();
    log.insert({ 
      pm25,
      pm10,
      pm1,
      temperature,
      humidity,
      timestamp: Date.now()
    });
    
    await producer.send({
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
  }
  if(Date.now() >= (lastLog + (60 * 1000 * 30))){
    lastLog = Date.now();
    await producer.send({
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
