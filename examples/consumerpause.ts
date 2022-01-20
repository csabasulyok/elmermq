import { promisify } from 'util';
import connect from '../src';
import { MyDto } from './dto';

const sleep = promisify((timeout: number, callback: () => void) => setTimeout(callback, timeout));

(async () => {
  const conn = await connect({
    hostname: 'localhost',
    poolSize: 4,
  });

  await conn.assertQueue('my_queue');

  let sub = await conn.consumeQueue<MyDto>('my_queue', (message: MyDto) => {
    console.log('my_queue ->', message);
  });

  // toggle pause/resume on listening every 5 seconds
  setInterval(async () => {
    await sleep(5000);
    await conn.pauseListener(sub);
    await sleep(5000);
    sub = await conn.resumeListener(sub);
  }, 10000);
})();
