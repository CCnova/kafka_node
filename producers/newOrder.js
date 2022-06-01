class NewOrder {
  async run() {
    const producer = kafka.producer();
    const value = "132123,67523,7894589745";
    await producer.connect();
    producer
      .send({
        topic: "ECOMMERCE_NEW_ORDER",
        messages: [{ key: value, value }],
      })
      .then(([value]) => {
        console.log(
          `success sending ${value.topicName} ::partition ${value.partition}/ offset ${value.baseOffset}/ timestamp ${value.timestamp}`
        )
      }
      )
      .catch((error) => console.log(error));
  }
}

// (async function () {
//   const newOrder = new NewOrder();
//   await newOrder.run();
// })();

export default (producer) => ({
  topic: 'ECOMMERCE_NEW_ORDER',

  send(messages) {
    return producer.send({
      topic: this.topic,
      messages
    });
  }

});

