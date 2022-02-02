import connect from '../src';
import { MyDto } from './dto';

(async () => {
  const conn = await connect();

  // temporary queue will also get recreated when resubscribing
  await conn.assertQueue('temp_queue', { exclusive: true, autoDelete: true });

  // automatic JSON deserialization
  conn.consumeQueue<MyDto>('temp_queue', (message: MyDto) => {
    console.log('temp_queue ->', message);
  });
})();
