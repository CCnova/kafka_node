export default (producer) => ({
  topic: 'ECOMMERCE_NEW_ORDER',

  send(messages) {
    return producer.send({
      topic: this.topic,
      messages
    });
  }

});

