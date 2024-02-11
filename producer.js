const { Kafka } = require('kafkajs');

// Initialize Kafka client
const kafka = new Kafka({
    clientId: 'my-kafka-producer',
    brokers: ['localhost:9092'], // Replace with your Kafka broker address
});

// Create a Kafka producer
const producer = kafka.producer();

// Connect the producer
const runProducer = async () => {
    await producer.connect();
};

// Publish a message
const publishMessage = async () => {
    const topic = 'my-topic'; // Your Kafka topic
    const message = {
        key: 'my-key', // Optional: specify a message key
        value: 'Hello, Kafka!', // Message content
    };

    await producer.send({
        topic,
        messages: [message],
    });
};

// Run the producer
runProducer()
    .then(publishMessage)
    .then(() => console.log('Message published successfully'))
    .catch((err) => console.error('Error publishing message:', err))
    .finally(() => producer.disconnect());
