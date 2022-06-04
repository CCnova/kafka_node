import { ECOMMERCE_TOPICS } from "./constants.js";
import kafka from "./kafka.js";

const value = "132123,67523,7894589745";
const email = 'Thanks for your order! We are processing your order!';

(async function () {
  const producer = kafka.producer();
  await producer.connect();

  producer.send({
      topic: ECOMMERCE_TOPICS.ECOMMERCE_NEW_ORDER,
      messages: [{ key: value, value }],
    });

  producer.send({
    topic: ECOMMERCE_TOPICS.ECOMMERCE_NEW_EMAIL,
    messages: [{ key: email, value: email }]
  });
})();