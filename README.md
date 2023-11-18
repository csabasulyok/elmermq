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

# Environment variables

| Name                           | Type     | Default value | Description                                    |
| ------------------------------ | -------- | ------------- | ---------------------------------------------- |
| ELMERMQ_PROTOCOL               | `string` | `amqp`        | Protocol (amqp or amqps)                       |
| ELMERMQ_HOSTNAME               | `string` | `localhost`   | Host of running RabbitMQ                       |
| ELMERMQ_PORT                   | `number` | 5672          | Port of running RabbitMQ                       |
| ELMERMQ_USERNAME               | `string` | `guest`       | Username for running RabbitMQ                  |
| ELMERMQ_PASSWORD               | `string` | `guest`       | Password for running RabbitMQ                  |
| ELMERMQ_PASSWORD_FILE          | `string` | *none*        | File variant of password                       |
| ELMERMQ_CONNECTION_LABEL       | `string` | `elmermq`     | Connection label (shown in management console) |
| ELMERMQ_RECONNECT_INTERVAL     | `number` | 5000          | Time in ms to delay when reconnecting          |
| ELMERMQ_RECONNECT_NUM_ATTEMPTS | `number` | 10            | Number of retries when attempting reconnecting |
| ELMERMQ_POOL_SIZE              | `number` | 1             | Size of pool                                   |