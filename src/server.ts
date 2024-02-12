import "dotenv/config";
import {Kafka, KafkaConfig, Partitioners} from 'kafkajs';
import {readFileSync} from 'fs';
import {dirname, join} from 'path';
import {fileURLToPath} from 'url';

// ca-certificate.crt file should be in main directory (above src)
const caCertificate = readFileSync(join(dirname(fileURLToPath(import.meta.url)), '..', 'ca-certificate.crt')).toString();

export const connectToKafkaWithKafkajs = async () => {
    const kafkaConfig: KafkaConfig = {
        clientId: "client-id",
        brokers: [process.env.KAFKA_URI],
        authenticationTimeout: 10000,
        reauthenticationThreshold: 10000,
        sasl: {
            mechanism: 'plain', // scram-sha-256 or scram-sha-512
            username: process.env.KAFKA_USER,
            password: process.env.KAFKA_PASSWORD
        },
        connectionTimeout: 3000,
        ssl: {
            rejectUnauthorized: true,
            ca: [caCertificate], // Include the CA certificate
        }

    }
    const kafka = new Kafka(kafkaConfig)

    const producer = kafka.producer({createPartitioner: Partitioners.LegacyPartitioner})
    await producer.connect()
    console.log("CONNECTED TO KAFKA")
    const data = await producer.send({
        topic: 'kafka-poc-test-topic-1',
        messages: [
            {key: 'key1', value: 'hello world'},
        ],
    })
    console.log(data)
    await producer.disconnect();

    const consumer = kafka.consumer({groupId: 'test-group'})
    await consumer.connect()
    await consumer.subscribe({topic: "kafka-poc-test-topic-1", fromBeginning: true})
    await consumer.run({
        eachMessage: async ({topic, partition, message, heartbeat, pause}) => {
            console.log({
                key: message.key.toString(),
                value: message.value.toString(),
                headers: message.headers,
            })
        },
    })

    console.log("DONE SENDING MESSAGE")
}


connectToKafkaWithKafkajs().then(() => console.log("TEST FINISHED")).catch(console.error)