# ElmerMQ

Hunting the RabbitMQ - amqplib wrapper with auto-reconnect and channel pooling

```bash
npm i elmermq
```

*[ElmerMQ](https://github.com/csabasulyok/elmermq)* is a wrapper around [amqplib](https://amqp-node.github.io/amqplib/) with some added functionalities:

- Auto-reconnect with auto-resubscribe to any queues
- Channel pooling with round-robin selection
- Pausing/resuming subscriptions
- Automatic processing of JSON messages
- Externalization via [extol](https://github.com/csabasulyok/extol)
