import kafka from "./kafka.js";
import newOrder from "./producers/newOrder.js";

const value = "132123,67523,7894589745";

(async function () {
  const producer = kafka.producer();
  await producer.connect();
  const newOrderProducer = newOrder(producer);
  newOrderProducer
    .send([{ key: value, value }])
    .then((result) => console.log(result));
})();
