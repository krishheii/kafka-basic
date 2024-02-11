const { Kafka } = require('kafkajs');

// Initialize Kafka client
const kafka = new Kafka({
    clientId: 'my-kafka-consumer',
    brokers: ['localhost:9092'], // Replace with your Kafka broker address
});

// Create a Kafka consumer
const consumer = kafka.consumer({ groupId: 'my-group' });

// Subscribe to the topic
const topic = 'my-topic'; // Your Kafka topic
consumer.subscribe({ topic });

// Handle incoming messages
const runConsumer = async () => {
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(`Received message from topic ${topic}, partition ${partition}: ${message.value.toString()}`);
            // Add your custom logic here to process the message
        },
    });
};

// Start the consumer
runConsumer().catch((err) => {
    console.error('Error in Kafka consumer:', err);
});
