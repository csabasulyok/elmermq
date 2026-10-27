import connect from '../src/index.js';
import { MyDto } from './dto.js';

// connect with same parameters as amqplib
// with added channel count for pool
const conn = await connect({
  hostname: 'localhost',
  poolSize: 4,
});

await conn.assertQueue('my_queue');
await conn.assertExchange('my_fanout', 'fanout');

// automatic JSON deserialization
await conn.consumeQueue<MyDto>('my_queue', (message: MyDto) => {
  console.log('my_queue ->', message.idx, message.date);
});

// temp queue connected for fanout
await conn.consume<MyDto>('my_fanout', '', (message: MyDto) => {
  console.log('my_fanout ->', message.idx, message.date);
});
