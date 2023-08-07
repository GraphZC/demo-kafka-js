import { Kafka } from "kafkajs";

// Create kafka instance
const kafka = new Kafka({
    brokers: ["localhost:58560"],
});

// Create consumer instance
const consumer = kafka.consumer({ groupId: "score-group" });

// Create scores object
const scores = {
    leagueA: {
        0: {
            team1: "",
            team2: "",
            time: 0,
            score: 0,
        },
        1: {
            team1: "",
            team2: "",
            time: 0,
            score: 0,
        }
    },
    leagueB: {
        0: {
            team1: "",
            team2: "",
            time: 0,
            score: 0,
        },
        1: {
            team1: "",
            team2: "",
            time: 0,
            score: 0,
        }
    }
};

// Show score board
const showMessages = (topic, partition, message) => {
    const score = JSON.parse(message.value.toString());
    scores[topic][partition] = score;
    console.clear();

    console.log("====== League A ======");
    for (let i = 0; i < 2; i++) {
        if (scores.leagueA[i].team1 === "") continue;
        console.log(
            `Team ${scores.leagueA[i].team1} vs ${scores.leagueA[i].team2} scored ${scores.leagueA[i].score} at ${scores.leagueA[i].time} minutes`
        );
    }
    console.log("====== League B ======");
    for (let i = 0; i < 2; i++) {
        if (scores.leagueB[i].team1 === "") continue;
        console.log(
            `Team ${scores.leagueB[i].team1} vs ${scores.leagueA[i].team2} scored ${scores.leagueB[i].score} at ${scores.leagueB[i].time} minutes`
        );
    }

}

// Consume score
const consumeScore = async () => {
    await consumer.connect()
    // Subscribe to topic 'leagueA' and 'leagueB'
    await consumer.subscribe({ topics: ['leagueA', 'leagueB'], fromBeginning: true})
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            showMessages(topic, partition, message);
        },
    })
}

console.log("Waiting for score...")

consumeScore();