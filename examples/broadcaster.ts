import connect from '../src/index.js';
import { MyDto } from './dto.js';

// connect with same parameters as amqplib
// with added channel count for pool
const conn = await connect({
  hostname: 'localhost',
  poolSize: 4,
});

// assertions directly through connection
// use channel pool in the background
await conn.assertQueue('my_queue');
await conn.assertExchange('my_fanout', 'fanout');

// message counters
const counters = { queue: 0, fanout: 1000 };

setInterval(() => {
  const message: MyDto = {
    idx: counters.queue,
    name: 'Hello to queue',
    age: 42,
    date: new Date(),
  };
  counters.queue += 1;

  // automatic JSON serialization when publishing
  conn.sendToQueue('my_queue', message);
  console.log('my_queue <-', message.idx, message.date);
}, 2000);

setInterval(() => {
  const message: MyDto = {
    idx: counters.fanout,
    name: 'Hello to fanout',
    age: 42,
    date: new Date(),
  };
  counters.fanout += 1;

  conn.publish('my_fanout', '', message);
  console.log('my_fanout <-', message.idx, message.date);
}, 7500);
