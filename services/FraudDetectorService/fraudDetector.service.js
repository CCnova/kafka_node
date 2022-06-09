import { ECOMMERCE_GROUPS, ECOMMERCE_TOPICS, TERMINAL_COLORS } from "../../constants.js";
import kafka from "../../kafka.js";
import { delay } from "../../utils.js";

const generateService = async (kafkaInstance) => {
  const consumer = kafkaInstance.consumer({ groupId: ECOMMERCE_GROUPS.FRAUD_DETECTION });
  const producer = kafkaInstance.producer();

  const service = {
    consumer,
    producer,

    subscribe(topics) {
      return this.consumer.subscribe({ topics });
    },

    isFraud(order) {
      return order.value > 120;
    },

    async handleOrder({ batch }) {
      for (let message of batch.messages) {
        const key = message.key?.toString("utf-8");
        const order = JSON.parse(message.value);
        const timestamp = message.timestamp;

        console.log("----------- Checking for fraud ----------");
        console.log(`Partition: ${batch.partition}`);
        console.log(`Key: ${key}`);
        console.log(`Value: ${order.currency}${order.value}`);
        console.log(`Time stamp: ${timestamp}`);

        if (this.isFraud(order)) {
          console.log(`${TERMINAL_COLORS.BgRed}%s\x1b[0m`, "----------- Fraud detected ----------");
          await this.producer.send({
            topic: ECOMMERCE_TOPICS.ORDER_REJECTED,
            messages: [{ key, value: JSON.stringify(order) }],
          });
          return;
        }
        console.log("---------- Order validated! -----------");

        await this.producer.send({
          topic: ECOMMERCE_TOPICS.ORDER_APPROVED,
          messages: [{ key, value: JSON.stringify(order) }],
        });
        return;
      }
    },

    async run(topics) {
      await this.subscribe(topics);
      await this.producer.connect();
      return this.consumer.run({
        autoCommitThreshold: 1,
        eachBatch: this.handleOrder.bind(this),
      });
    },
  };

  return service;
};

generateService(kafka).then(service => service.run([ECOMMERCE_TOPICS.NEW_ORDER]));