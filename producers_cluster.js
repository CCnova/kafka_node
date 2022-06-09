import { ECOMMERCE_TOPICS } from "./constants.js";
import kafka from "./kafka.js";
import crypto from 'crypto';


const email = 'Thanks for your order! We are processing your order!';
const interval = 1000;

(async function () {
  const producer = kafka.producer();
  await producer.connect();

  setInterval(() => {
    const order = {
      items: ['item1', 'item2', 'item3'],
      value: Math.floor(Math.random() * 200),
      currency: 'USD',
      buyer: 'John Doe',
    };
    const key = crypto.randomUUID();
    try {
      producer.send({
          topic: ECOMMERCE_TOPICS.NEW_ORDER,
          messages: [{ key, value: JSON.stringify(order) }],
        }).then(_ => console.log('NEW MESSAGE SENDED!'));

      // producer.send({
      //   topic: ECOMMERCE_TOPICS.SEND_EMAIL,
      //   messages: [{ key, value: email }]
      // }).then(_ => console.log('NEW MESSAGE SENDED!'));
    } catch (e) {
      console.error(e);
    }
  }, interval);
})();