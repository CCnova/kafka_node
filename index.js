import fraudDetectorService from "./consumers/fraudDetectorService.js";
import kafka from "./kafka.js";
import newOrder from "./producers/newOrder.js";

const value = "132123,67523,7894589745";

(async function () {
  const fraudDetector = await fraudDetectorService(kafka);
  await fraudDetector.subscribe(['ECOMMERCE_NEW_ORDER'])
  await fraudDetector.run();

  const producer = kafka.producer();
  const newOrderProducer = newOrder(producer);
  await producer.connect();
  newOrderProducer
    .send([{ key: value, value }])

})();
