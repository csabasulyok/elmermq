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

  // automatic queue assertion and JSON deserialization
  conn.onQueueMessage<MyDto>('my_queue', (message: MyDto) => {
    console.log('my_queue ->', message);
  });

  // temp queue connected for fanout
  conn.onFanoutMessage<MyDto>('my_fanout', (message: MyDto) => {
    console.log('my_fanout ->', message);
  });
})();
