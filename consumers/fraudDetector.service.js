import { ECOMMERCE_GROUPS, ECOMMERCE_TOPICS } from "../constants.js";
import kafka from "../kafka.js";

const generateService = async (kafkaInstance) => {
  const consumer = kafkaInstance.consumer({ groupId: ECOMMERCE_GROUPS.FRAUD_DETECTION });
  await consumer.connect();

  return {
    consumer,

    subscribe(topics) {
      return this.consumer.subscribe({ topics });
    },

    async handleBatch({ batch }) {
      for (let message of batch.messages) {
        const key = message.key?.toString("utf-8");
        const value = message.value?.toString("utf-8");
        const timestamp = message.timestamp;

        console.log("----------- Checking for fraud ----------");
        console.log(`Partition: ${batch.partition}`);
        console.log(`Key: ${key}`);
        console.log(`Value: ${value}`);
        console.log(`Time stamp: ${timestamp}`);
      }
    },

    async run(topics) {
      await this.subscribe(topics);
      return this.consumer.run({
        eachBatch: this.handleBatch,
      });
    },
  };
};

generateService(kafka).then(service => service.run([ECOMMERCE_TOPICS.NEW_ORDER]));