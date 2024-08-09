const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:9092"],
});

const topicName = "test_happy";

let producer;

const connect = async () => {
  producer = kafka.producer();
  await producer.connect();
  const processProducer = async (index, partition) => {
    await producer.send({
      topic: topicName,
      messages: [{ value: index + "", partition }],
    });
  };

  for (let i = 0; i < 25; i++) {
    processProducer(i, i).then(() => {
      console.log("done");
    });
  }
};

connect();
