import { Kafka } from "kafkajs";

export default new Kafka({
  brokers: ['127.0.0.1:9092']
});