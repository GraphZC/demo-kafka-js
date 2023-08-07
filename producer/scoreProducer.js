import { Kafka, Partitioners } from 'kafkajs';

// Create kafka instance
const kafka = new Kafka({
    brokers: ["localhost:58560"],
});

// Create producer instance
const producer = kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner
});

// A1 vs A2
const dataLeageA1A2 = [
    {team1: 'A1', team2: 'A2', time: 3, score: '1-0'},
    {team1: 'A1', team2: 'A2', time: 25, score: '1-1'},
    {team1: 'A1', team2: 'A2', time: 52, score: '1-2'},
    {team1: 'A1', team2: 'A2', time: 90, score: '0-1'},
];

// A3 vs A4
const dataLeageA3A4 = [
    {team1: 'A3', team2: 'A4', time: 30, score: '0-2'},
    {team1: 'A3', team2: 'A4', time: 51, score: '1-2'},
    {team1: 'A3', team2: 'A4', time: 60, score: '1-2'},
    {team1: 'A3', team2: 'A4', time: 90, score: '1-2'},
];


// B1 vs B2
const dataLeageB1B2 = [
    {team1: 'B1', team2: 'B2', time: 30, score: '1-0'},
    {team1: 'B1', team2: 'B2', time: 40, score: '2-0'},
    {team1: 'B1', team2: 'B2', time: 90, score: '2-0'},
];

// B3 vs B4
const dataLeageB3B4 = [
    {team1: 'B3', team2: 'B4', time: 23, score: '1-0'},
    {team1: 'B3', team2: 'B4', time: 56, score: '1-1'},
    {team1: 'B3', team2: 'B4', time: 90, score: '1-1'},
];

const produceScore = async (topic, data, partition) => {
    
    // Send each message to kafka
    for (const msg of data) {
        await producer.connect();
        await producer.send({
            topic: topic,
            messages: [{ 
                partition: partition,
                value: JSON.stringify(msg) 
            }],
        });
        await producer.disconnect();
        await new Promise(resolve => setTimeout(resolve, 2000))
    }

}

// Produce score
const produce = async () => {
    await Promise.all([
        produceScore('leagueA', dataLeageA1A2, 0),
        produceScore('leagueA', dataLeageA3A4, 1),
        produceScore('leagueB', dataLeageB1B2, 0),
        produceScore('leagueB', dataLeageB3B4, 1),
    ]);
}

produce();