import { Connection, Channel, ConsumeMessage, Options, Replies } from 'amqplib';
import yall from 'yall2';
import autoBind from 'auto-bind';
import { ConsumeOptions, ChannelMessageCallback, OnExchangeOptions, OnQueueOptions, PublishOptions } from './api';
import id from './util';
import Queue from './queue';

/**
 * Description of a subscription sufficient to re-subscribe or pause/resume
 */
export type ChannelSubscription = {
  consumerTag: string;
  source: string;
  resubscribe: () => Promise<string>;
  active: boolean;
};

/**
 * Message to be published, with all destination information.
 * Can be queued and left to be sent out later if channel disconnects.
 */
export type PublishMessage<T> = {
  destination?: string;
  routingKey: string;
  message: T;
  options?: PublishOptions;
};

/**
 * Wrapper around AMQP channel which remembers its own susbcriptions
 * and is re-bindable to a new connection, when reconnection is necessary
 */
export default class ChannelWithRetention {
  private channel: Channel;
  private subscriptions: Record<string, ChannelSubscription>;
  private backedUpQueue: Queue<PublishMessage<unknown>>;

  name: string;
  connected: boolean;

  constructor(name: string) {
    this.name = name;
    this.connected = false;
    this.subscriptions = {};
    this.backedUpQueue = new Queue();
    autoBind(this);
  }

  async connect(connection: Connection): Promise<void> {
    this.channel = await connection.createChannel();
    this.channel.prefetch(1);

    this.channel.on('close', () => {
      this.connected = false;
    });

    this.connected = true;
    // if this is a reconnection, reconnect all previously active subscriptions
    await Promise.all(
      Object.values(this.subscriptions)
        .filter((s) => s?.active)
        .map((s) => s?.resubscribe()),
    );
    // if this is a reconnection, send out any messages queued up while disconnected
    while (this.backedUpQueue.backedUp) {
      this.publish(this.backedUpQueue.consume());
    }
  }

  async close(): Promise<void> {
    try {
      await this.channel.close();
    } catch (err) {
      // channel already closing because the parent connection is, so it can be ignored
      if (err?.message !== 'Channel closing') {
        throw err;
      }
    }
  }

  //
  // Wrapper messages
  //

  async assertQueue(queue: string, options?: Options.AssertQueue): Promise<Replies.AssertQueue> {
    const ret = await this.channel.assertQueue(queue, options);
    return ret;
  }

  async deleteQueue(queue: string, options?: Options.DeleteQueue): Promise<Replies.DeleteQueue> {
    const ret = await this.channel.deleteQueue(queue, options);
    return ret;
  }

  async bindQueue(queue: string, source: string, pattern: string, args?: Record<string, unknown>): Promise<Replies.Empty> {
    const ret = await this.channel.bindQueue(queue, source, pattern, args);
    return ret;
  }

  async unbindQueue(queue: string, source: string, pattern: string, args?: Record<string, unknown>): Promise<Replies.Empty> {
    const ret = await this.channel.unbindQueue(queue, source, pattern, args);
    return ret;
  }

  async assertExchange(exchange: string, type: string, options?: Options.AssertExchange): Promise<Replies.AssertExchange> {
    const ret = await this.channel.assertExchange(exchange, type, options);
    return ret;
  }

  async deleteExchange(exchange: string, options?: Options.DeleteExchange): Promise<Replies.Empty> {
    const ret = await this.channel.deleteExchange(exchange, options);
    return ret;
  }

  async bindExchange(destination: string, source: string, pattern: string, args?: Record<string, unknown>): Promise<Replies.Empty> {
    const ret = await this.channel.bindExchange(destination, source, pattern, args);
    return ret;
  }

  async unbindExchange(destination: string, source: string, pattern: string, args?: Record<string, unknown>): Promise<Replies.Empty> {
    const ret = await this.channel.unbindExchange(destination, source, pattern, args);
    return ret;
  }

  //
  // Common convenience method for processing a message
  //

  private async addConsumer<T>(queue: string, callback: ChannelMessageCallback<T>, options?: ConsumeOptions): Promise<Replies.Consume> {
    const response = await this.channel.consume(
      queue,
      async (message: ConsumeMessage) => {
        if (!message) {
          return;
        }

        try {
          yall.debug(`[${queue}] consuming message of length ${message.content.length}`);

          let body: T;
          if (options?.raw) {
            // if raw, pass buffer as is
            body = message.content as unknown as T;
          } else {
            // deserialize JSON
            body = JSON.parse(message.content.toString()) as T;
          }
          yall.debug(body);

          await callback(body, message.properties);
          this.channel.ack(message);
        } catch (e) {
          yall.error(`[${queue}] error, ${e}`);
          // console.error(e);
          this.channel.reject(message, false);
        }
      },
      options,
    );
    return response;
  }

  //
  // Helpers for different queue/exchange types
  //

  async onQueueMessage<T>(queue: string, callback: ChannelMessageCallback<T>, options?: OnQueueOptions): Promise<string> {
    // assert queue with given parameters
    await this.channel.assertQueue(queue, options?.assert);
    // build callback
    const { consumerTag } = await this.addConsumer(queue, callback, options?.consume);
    // register subscription so resubscription is possible with other connection/channel
    this.subscriptions[consumerTag] = {
      consumerTag,
      source: queue,
      active: true,
      resubscribe: () => this.onQueueMessage(queue, callback, options),
    };

    yall.info(`Subscribed to messages from queue ${queue}`);
    return consumerTag;
  }

  async onFanoutMessage<T>(fanout: string, callback: ChannelMessageCallback<T>, options?: OnExchangeOptions): Promise<string> {
    // assert fanout
    await this.channel.assertExchange(fanout, 'fanout', options?.assert);
    // assert temporary queue with given parameters
    const queue = `${fanout}-${id()}`;
    await this.channel.assertQueue(queue, { exclusive: true, autoDelete: true });
    await this.channel.bindQueue(queue, fanout, '');
    // build callback
    const { consumerTag } = await this.addConsumer(queue, callback, options?.consume);
    // register subscription so resubscription is possible with other connection/channel
    this.subscriptions[consumerTag] = {
      consumerTag,
      source: fanout,
      active: true,
      resubscribe: () => this.onFanoutMessage(fanout, callback, options),
    };

    yall.info(`Subscribed to fanout ${fanout} via temp queue ${queue}`);
    return consumerTag;
  }

  async onTopicMessage<T>(
    topic: string,
    pattern: string,
    callback: ChannelMessageCallback<T>,
    options?: OnExchangeOptions,
  ): Promise<string> {
    // assert fanout
    await this.channel.assertExchange(topic, 'topic', options?.assert);
    // assert temporary queue with given parameters
    const queue = `${topic}-${id()}`;
    await this.channel.assertQueue(queue, { exclusive: true, autoDelete: true });
    await this.channel.bindQueue(queue, topic, pattern);
    // build callback
    const { consumerTag } = await this.addConsumer(queue, callback, options?.consume);
    // register subscription so resubscription is possible with other connection/channel
    this.subscriptions[consumerTag] = {
      consumerTag,
      source: topic,
      active: true,
      resubscribe: () => this.onFanoutMessage(topic, callback, options),
    };

    yall.info(`Subscribed to topic ${topic}:${pattern} via temp queue ${queue}`);
    return consumerTag;
  }

  //
  // Common convenience method for publishing a message
  //

  private publish<T>(publishMessage: PublishMessage<T>): boolean {
    const { destination, routingKey, message, options } = publishMessage;
    const body = options?.raw ? (message as unknown as Buffer) : Buffer.from(JSON.stringify(message));

    // if connected, send directly
    if (this.connected) {
      if (!destination) {
        return this.channel.sendToQueue(routingKey, body, options);
      }
      return this.channel.publish(destination, routingKey, body, options);
    }

    // disconnected - will try to reconnect, in the meantime queue up message
    this.backedUpQueue.push(publishMessage);
    return true;
  }

  //
  // Helpers for different queue/exchange types
  //

  publishToQueue<T>(queue: string, message: T, options?: PublishOptions): boolean {
    return this.publish({
      routingKey: queue,
      message,
      options,
    });
  }

  publishToDirect<T>(direct: string, routingKey: string, message: T, options?: PublishOptions): boolean {
    return this.publish({
      destination: direct,
      routingKey,
      message,
      options,
    });
  }

  publishToFanout<T>(fanout: string, message: T, options?: PublishOptions): boolean {
    return this.publish({
      destination: fanout,
      routingKey: '',
      message,
      options,
    });
  }

  publishToTopic<T>(topic: string, routingKey: string, message: T, options?: PublishOptions): boolean {
    return this.publish({
      destination: topic,
      routingKey,
      message,
      options,
    });
  }

  //
  // Pausing/resuming
  //

  isListenerActive(consumerTag: string): boolean {
    return this.subscriptions[consumerTag]?.active;
  }

  async pauseListener(consumerTag: string): Promise<boolean> {
    if (!(consumerTag in this.subscriptions)) {
      yall.warn(`Trying to pause non-existent subscription ${consumerTag}`);
      return false;
    }

    const { source, active } = this.subscriptions[consumerTag];

    if (!active) {
      yall.warn(`Trying to pause already paused subscription ${source}`);
      return false;
    }

    yall.info(`Unsubscribing from ${source} (tag ${consumerTag})`);
    await this.channel.cancel(consumerTag);
    this.subscriptions[consumerTag].active = false;
    return true;
  }

  async resumeListener(consumerTag: string): Promise<string> {
    if (!(consumerTag in this.subscriptions)) {
      yall.warn(`Trying to resume non-existent subscription ${consumerTag}`);
      return undefined;
    }

    const { source, active } = this.subscriptions[consumerTag];

    if (active) {
      yall.warn(`Trying to resume already active subscription to ${source}`);
      return undefined;
    }

    yall.info(`Resubscribing to ${source}`);
    const newConsumerTag = await this.subscriptions[consumerTag].resubscribe();
    delete this.subscriptions[consumerTag];
    return newConsumerTag;
  }

  async stopListener(consumerTag: string): Promise<boolean> {
    if (!(consumerTag in this.subscriptions)) {
      yall.warn(`Trying to stop listening on non-existent subscription ${consumerTag}`);
      return false;
    }

    await this.channel.cancel(consumerTag);
    delete this.subscriptions[consumerTag];
    return true;
  }
}
