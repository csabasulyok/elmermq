import connect from '../src';

// message format we expect on a queue
type MyDto = {
  name: string;
  age: number;
};

(async () => {
  // connect with same parameters as amqplib
  // with added channel count for pool
  const conn = await connect({
    hostname: 'localhost',
    poolSize: 4,
  });

  setInterval(() => {
    // automatic JSON serialization when publishing
    console.log('Sending message to my_queue');
    conn.publishToQueue<MyDto>('my_queue', {
      name: 'Hello',
      age: 42,
    });
  }, 2000);

  setInterval(() => {
    // automatic JSON serialization when publishing
    console.log('Sending message to my_fanout');
    conn.publishToFanout<MyDto>('my_fanout', {
      name: 'Hello',
      age: 42,
    });
  }, 7500);
})();
