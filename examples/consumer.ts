import connect from '../src';
import { MyDto } from './dto';

(async () => {
  // connect with same parameters as amqplib
  // with added channel count for pool
  const conn = await connect({
    hostname: 'localhost',
    poolSize: 4,
  });

  await conn.assertQueue('my_queue');
  await conn.assertExchange('my_fanout', 'fanout');

  // automatic JSON deserialization
  conn.consumeQueue<MyDto>('my_queue', (message: MyDto) => {
    console.log('my_queue ->', message);
  });

  // temp queue connected for fanout
  conn.consume<MyDto>('my_fanout', undefined, (message: MyDto) => {
    console.log('my_fanout ->', message);
  });
})();
