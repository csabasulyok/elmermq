# ElmerMQ

Hunting the RabbitMQ - amqplib wrapper with auto-reconnect and channel pooling

```bash
npm i elmermq
```

> Since version 2, ElmerMQ is an ES module only (no CommonJS build), requires Node.js 24+ and is built on amqplib 2. Both `import connect from 'elmermq'` and `import { connect } from 'elmermq'` work.

_[ElmerMQ](https://github.com/csabasulyok/elmermq)_ is a wrapper around [amqplib](https://amqp-node.github.io/amqplib/) with some added functionalities:

- Auto-reconnect with auto-resubscribe to any queues
- Channel pooling with round-robin selection
- Pausing/resuming subscriptions
- Automatic processing of JSON messages
- Named queues that outlive their consumers, and requeueing from callbacks
- Externalization via [extol](https://github.com/csabasulyok/extol)

# Named queues and requeueing

By default, `consume(exchange, pattern, callback)` binds a temporary exclusive, auto-deleted queue, which loses messages published while nobody consumes. With `queueName`, the queue has a fixed name, is not exclusive and is not auto-deleted, so it keeps collecting messages between consumers (e.g. reconnects of the same client). Use `queueOptions.expires` so RabbitMQ deletes it once unused:

```ts
await mb.consume('sitting-handout', 'sitting.1.2', callback, {
  queueName: 'sitting-handout-2.1.abc',
  queueOptions: { expires: 300_000 },
});
```

A message is acked when the callback finishes and rejected (discarded) when it throws. Throw a `RequeueError` to put the message back into the queue instead, e.g. when it could not be passed on yet:

```ts
import { RequeueError } from 'elmermq';

await mb.consume('exchange', 'pattern', async (message) => {
  if (!(await forward(message))) {
    throw new RequeueError('could not forward');
  }
});
```

# Environment variables

| Name                           | Type     | Default value | Description                                    |
| ------------------------------ | -------- | ------------- | ---------------------------------------------- |
| ELMERMQ_PROTOCOL               | `string` | `amqp`        | Protocol (amqp or amqps)                       |
| ELMERMQ_HOSTNAME               | `string` | `localhost`   | Host of running RabbitMQ                       |
| ELMERMQ_PORT                   | `number` | 5672          | Port of running RabbitMQ                       |
| ELMERMQ_USERNAME               | `string` | `guest`       | Username for running RabbitMQ                  |
| ELMERMQ_PASSWORD               | `string` | `guest`       | Password for running RabbitMQ                  |
| ELMERMQ_PASSWORD_FILE          | `string` | _none_        | File variant of password                       |
| ELMERMQ_CONNECTION_LABEL       | `string` | `elmermq`     | Connection label (shown in management console) |
| ELMERMQ_RECONNECT_INTERVAL     | `number` | 5000          | Time in ms to delay when reconnecting          |
| ELMERMQ_RECONNECT_NUM_ATTEMPTS | `number` | 10            | Number of retries when attempting reconnecting |
| ELMERMQ_POOL_SIZE              | `number` | 1             | Size of pool                                   |
