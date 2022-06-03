export default async (kafkaInstance) => {
  const consumer = kafkaInstance.consumer({ groupId: "FRAUD_DETECTION" });
  await consumer.connect();

  return {
    consumer,

    subscribe(topics) {
      return this.consumer.subscribe({ topics });
    },

    run() {
      return this.consumer.run({
        eachBatchAutoResolve: true,
        eachBatch: async ({ batch }) => {
          console.log('EACH BATCH');
          for (let message of batch.messages) {
            const key = message.key?.toString("utf-8");
            const value = message.value?.toString("utf-8");
            const timestamp = message.timestamp;

            console.log("----------- Checking for fraud ----------");
            console.log(`Key: ${key}`);
            console.log(`Value: ${value}`);
            console.log(`Time stamp: ${timestamp}`);
          }
        },
      });
    },
  };
};
