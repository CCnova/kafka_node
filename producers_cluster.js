import { ECOMMERCE_TOPICS } from "./constants.js";
import kafka from "./kafka.js";
import crypto from 'crypto';

const value = "132123,67523,7894589745";
const email = 'Thanks for your order! We are processing your order!';
const interval = 1000;

(async function () {
  const producer = kafka.producer();
  await producer.connect();

  setInterval(() => {
    const key = crypto.randomUUID();
    producer.send({
        topic: ECOMMERCE_TOPICS.NEW_ORDER,
        messages: [{ key, value }],
      }).then(_ => console.log('NEW MESSAGE SENDED!'));

    producer.send({
      topic: ECOMMERCE_TOPICS.SEND_EMAIL,
      messages: [{ key, value: email }]
    }).then(_ => console.log('NEW MESSAGE SENDED!'));
  }, interval);
})();