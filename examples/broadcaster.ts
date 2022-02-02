import connect from '../src';
import { MyDto } from './dto';

(async () => {
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

  let queueIdx = 0;
  setInterval(() => {
    const message: MyDto = {
      idx: queueIdx,
      name: 'Hello to queue',
      age: 42,
      date: new Date(),
    };
    queueIdx += 1;

    // automatic JSON serialization when publishing
    conn.sendToQueue('my_queue', message);
    console.log('my_queue <-', message.idx, message.date);
  }, 2000);

  let fanoutIdx = 1000;
  setInterval(() => {
    const message: MyDto = {
      idx: fanoutIdx,
      name: 'Hello to fanout',
      age: 42,
      date: new Date(),
    };
    fanoutIdx += 1;

    conn.publish('my_fanout', undefined, message);
    console.log('my_fanout <-', message.idx, message.date);
  }, 7500);
})();
