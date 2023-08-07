import { Kafka } from "kafkajs";
import csvWriter from "../services/csvWrite.js";

// Create kafka instance
const kafka = new Kafka({
    brokers: ["localhost:58560"],
});

// Create consumer instance
const consumer = kafka.consumer({ groupId: "league-a-group" });

// Consume score
const consumeScore = async () => {
    await consumer.connect()
    // Subscribe to topic 'leagueA'
    await consumer.subscribe({ topic: 'leagueA', fromBeginning: true})
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            csvWriter('leagueA.csv', JSON.parse(message.value.toString()));
        },
    })
}

console.log("Waiting for score...")

consumeScore();