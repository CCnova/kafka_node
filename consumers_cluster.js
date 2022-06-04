import { ECOMMERCE_TOPICS } from "./constants.js";
import EmailService from "./consumers/email.service.js";
import FraudDetectorService from "./consumers/fraudDetector.service.js";
import kafka from "./kafka.js";

(async function () {
  const fraudDetectorService = await FraudDetectorService(kafka);
  await fraudDetectorService.subscribe([ECOMMERCE_TOPICS.NEW_ORDER]);
  await fraudDetectorService.run();

  const emailService = await EmailService(kafka);
  await emailService.subscribe([ECOMMERCE_TOPICS.SEND_EMAIL]);
  await emailService.run();
})();