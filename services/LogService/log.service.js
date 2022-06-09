import { ECOMMERCE_GROUPS, ECOMMERCE_TOPICS, prefixes } from "../../constants.js";
import kafka from "../../kafka.js";
import fs from 'fs';
import { fileURLToPath, URL } from "url";

const __dirname = fileURLToPath(new URL('.', import.meta.url));
console.log(__dirname);

const writeStream = fs.createWriteStream(`${__dirname}/log.txt`, 'utf8');

const generateService = async (kafkaInstance) => {
  const consumer = kafkaInstance.consumer({ groupId: ECOMMERCE_GROUPS.LOG });
  await consumer.connect();

  return {
    consumer,

    subscribe(topics) {
      return this.consumer.subscribe({ topics });
    },

    async handleBatch({ batch }) {
      for (let message of batch.messages) {
        writeStream.write(message.value);
        console.log(message.value)
        // const key = message.key?.toString("utf-8");
        // const value = message.value?.toString("utf-8");
        // const timestamp = message.timestamp;
        // console.log("----------- LOG ----------");
        // console.log(`Topic: ${batch.topic}`);
        // console.log(`Key: ${key}`);
        // console.log(`Value: ${value}`);
        // console.log(`Time stamp: ${timestamp}`);
      }
    },

    async run(topics) {
      await this.subscribe(topics);
      return this.consumer.run({
        eachBatchAutoResolve: true,
        eachBatch: this.handleBatch,
      });
    },
  };
};

generateService(kafka).then(service => service.run([new RegExp(`${prefixes.ECOMMERCE}.*`, 'g')]));
// generateService(kafka).then(service => service.run([ECOMMERCE_TOPICS.ORDER_REJECTED]));