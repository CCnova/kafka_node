import fraudDetectorService from "./consumers/fraudDetector.service.js";
import kafka from "./kafka.js";

(async function () {
  const fraudDetector = await fraudDetectorService(kafka);
  await fraudDetector.subscribe(["ECOMMERCE_NEW_ORDER"]);
  await fraudDetector.run();
})();