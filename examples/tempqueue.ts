import connect from '../src/index.js';
import { MyDto } from './dto.js';

const conn = await connect();

// temporary queue will also get recreated when resubscribing
await conn.assertQueue('temp_queue', { exclusive: true, autoDelete: true });

// automatic JSON deserialization
await conn.consumeQueue<MyDto>('temp_queue', (message: MyDto) => {
  console.log('temp_queue ->', message);
});
