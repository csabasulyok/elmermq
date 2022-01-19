import { Channel } from 'amqplib';

export type MessageBrokerSubscription = {
  channel: Channel;
  consumerTag: string;
  resubscribe: () => Promise<string>;
  active: boolean;
};
