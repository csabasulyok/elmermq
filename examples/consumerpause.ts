import { promisify } from 'util';
import connect from '../src';

const sleep = promisify((timeout: number, callback: () => void) => setTimeout(callback, timeout));

// message format we expect on a queue
type MyDto = {
  name: string;
  age: number;
};

(async () => {
  const conn = await connect({
    hostname: 'localhost',
    poolSize: 4,
  });

  let sub = await conn.onQueueMessage<MyDto>('my_queue', (message: MyDto) => {
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
