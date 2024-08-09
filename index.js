const cluster = require("node:cluster");
const os = require("os");

const cpuLength = os.cpus().length;

const topicName = "test_happy";

if (cluster.isPrimary) {
  for (let i = 0; i <= cpuLength; i++) {
    cluster.fork();
  }
  cluster.on("exit", (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died`);
  });
} else {
  const { Kafka } = require("kafkajs");

  const kafka = new Kafka({
    clientId: "my-app",
    brokers: ["localhost:9092"],
  });
  const processConsumer = async () => {
    const consumer = kafka.consumer({ groupId: "users" });

    await consumer.connect();

    console.log(`Worker ${process.pid} connected to Kafka`);

    await consumer.subscribe({ topic: topicName });
    const logMessage = async (counter, topic, partition, message) => {
      return new Promise((resolve) =>
        setTimeout(() => {
          console.log(
            `Worker ${process.pid} received a new message: ${counter} `,
            {
              topic,
              partition,
              message: {
                value: message.value.toString(),
              },
            }
          );
          resolve();
        }, 700)
      );
    };
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        let Counter = 1;
        await logMessage(Counter, topic, partition, message);
        Counter++;
      },
    });
  };
  processConsumer();
}


//Run admin js
//Run index js
//Run producer.js
