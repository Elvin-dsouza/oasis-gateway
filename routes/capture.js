var express = require('express');
var router = express.Router();
const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'oasis-sensor-log',
  brokers: [`localhost:29092`]
});


const logSensorInformation = async (pm25, pm10, pm1, temperature, humidity) => {
  const producer = kafka.producer()
  await producer.connect()
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
