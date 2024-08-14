const { Kafka } = require('kafkajs');

async function createKafkaLibrary(brokers, appId, appGroup, logger) {
  const kafka = new Kafka({
    clientId: appId,
    brokers: brokers
  });

  const admin = kafka.admin()
  const producer = kafka.producer();
  const consumers = {
    group: kafka.consumer({ groupId: appGroup }),
    individual: kafka.consumer({ groupId: appId })
  };

  const subscriptions = {
    group: new Map(),
    individual: new Map()
  };

  async function groupConsumerCallback({ topic, partition, message }) {
    if (subscriptions.group.has(topic)) {
      subscriptions.group.get(topic)({
        topic: topic,
        partition: partition,
        message: message.value.toString(),
        headers: message.headers
      })
    }
  }

  async function individualConsumerCallback({ topic, partition, message }) {
    if (subscriptions.individual.has(topic)) {
      subscriptions.individual.get(topic)({
        topic: topic,
        partition: partition,
        message: message.value.toString(),
        headers: message.headers
      })
    }
  }
  
  async function init() {
    try {
      await admin.connect()
      await producer.connect();
      await consumers.group.connect();
      await consumers.individual.connect();

      // Handle connection and disconnection events
      producer.on('producer.connect', () => logger.debug('Producer connected'))
      producer.on('producer.disconnect', () => logger.debug('Producer disconnected'))
      consumers.group.on('consumer.connect', () => logger.debug('Group consumer connected'))
      consumers.group.on('consumer.disconnect', () => logger.debug('Group consumer disconnected'))
      consumers.individual.on('consumer.connect', () => logger.debug('Individual consumer connected'))
      consumers.individual.on('consumer.disconnect', () => logger.debug('Individual consumer disconnected'))

      // Run consumers only once
      await consumers.group.run({
        eachMessage: groupConsumerCallback
      });

      await consumers.individual.run({
        eachMessage: individualConsumerCallback
      });

      return { produce, subscribeAsGroupMember, subscribeAsIndividual, unsubscribe, createTopic };
    } catch (err) {
      throw new Error('Failed to initialize Kafka library: ' + err.message);
    }
  }

  async function produce(topic, key, message, headers) {
    try {
      await producer.send({
        topic: topic,
        messages: [{ key: key, value: message, headers: headers }]
      });
    } catch (err) {
      throw new Error('Failed to produce message: ' + err.message);
    }
  }

  async function subscribeAsGroupMember(topics, callback) {
    try {
      await consumers.group.stop()
      for (const topic of topics) {
        if (subscriptions.individual.has(topic)) {
          throw new Error(`Topic ${topic} is already subscribed as an individual`);
        }
        subscriptions.group.set(topic, callback);
        await consumers.group.subscribe({ topic: topic, fromBeginning: false });
      }

      await consumers.group.run({
        eachMessage: groupConsumerCallback
      });

    } catch (err) {
      throw new Error('Failed to subscribe as group member: ' + err.message);
    }
  }

  async function subscribeAsIndividual(topics, callback) {
    try {
      await consumers.individual.stop()
      for (const topic of topics) {
        if (subscriptions.group.has(topic)) {
          throw new Error(`Topic ${topic} is already subscribed as a group member`);
        }
        subscriptions.individual.set(topic, callback);
        await consumers.individual.subscribe({ topic: topic, fromBeginning: false });
      }

      await consumers.individual.run({
        eachMessage: individualConsumerCallback
      })

    } catch (err) {
      throw new Error('Failed to subscribe as individual: ' + err.message);
    }
  }

  async function unsubscribe(topics) {
    try {
      for (const topic of topics) {
        if (subscriptions.group.has(topic)) {
          await consumers.group.unsubscribe({ topic: topic });
          subscriptions.group.delete(topic);
        }
        if (subscriptions.individual.has(topic)) {
          await consumers.individual.unsubscribe({ topic: topic });
          subscriptions.individual.delete(topic);
        }
      }
    } catch (err) {
      throw new Error('Failed to unsubscribe: ' + err.message);
    }
  }

  async function createTopic(topicName, numPartitions, replicationFactor) {
    try {
      await admin.createTopics({
        topics: [{
          topic: topicName,
          numPartitions: numPartitions,
          replicationFactor: replicationFactor,
        }],
      });
      logger.debug(`Topic ${topicName} created successfully`);
    } catch (err) {
      logger.error(`Failed to create topic ${topicName}:, error: ${err.message}`);
      throw err;
    }
  }

  return await init();
}

module.exports.createKafkaLibrary = createKafkaLibrary;
