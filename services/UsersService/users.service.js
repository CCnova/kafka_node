import { ECOMMERCE_TOPICS, ECOMMERCE_GROUPS } from "../../constants.js";
import kafka from "../../kafka.js";
import sqlite3 from 'sqlite3';
import { delay, getDirname } from "../../utils.js";
import { CREATE_TABLE_USER, INSERT_USER, SELECT_USER } from "./queries.js";
import crypto from 'crypto';

const __dirname = getDirname(import.meta.url);

const generateService = async (kafkaInstance) => {
  const consumer = kafkaInstance.consumer({ groupId: ECOMMERCE_GROUPS.USER });
  await consumer.connect();

  const database = new sqlite3.Database(`${__dirname}/database.db`);

  database.run(CREATE_TABLE_USER);

  return {
    consumer,

    subscribe(topics) {
      return this.consumer.subscribe({ topics });
    },

    async handleBatch({ batch }) {
      for (let message of batch.messages) {
        const key = message.key?.toString("utf-8");
        const {email, ...value} = JSON.parse(message.value);
        const timestamp = message.timestamp;

        if (await this.isNewUser(email)) this.createUser(email);
        else console.log('------------ USER ALREADY EXISTS ------------');
        await delay(2000);
      }
    },

    async run(topics) {
      await this.subscribe(topics);
      return this.consumer.run({
        eachBatchAutoResolve: true,
        eachBatch: this.handleBatch.bind(this),
      });
    },

    isNewUser(email) {
      return new Promise((resolve, reject) => {

        database.all(SELECT_USER, [email], (err, rows) => {
          if (err) reject(err);
          resolve(rows.length === 0);
        });

      });
    },

    async createUser(email) {
      try {
        database.run(INSERT_USER, [crypto.randomUUID(), email]);
      } catch(error) {
        console.error(error.message);
      }
    }
  };
}

generateService(kafka).then(service => service.run([ECOMMERCE_TOPICS.NEW_ORDER]));