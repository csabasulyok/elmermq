import { Connection, Channel, ConsumeMessage, Options, Replies } from 'amqplib';
import yall from 'yall2';
import autoBind from 'auto-bind';
import { ConsumeOptions, ChannelMessageCallback, PublishOptions } from './api';
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
  exchange?: string;
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
    this.flush();
  }

  flush(): void {
    // flush any messages queued up while disconnected
    while (this.backedUpQueue.backedUp) {
      const { exchange, routingKey, message, options } = this.backedUpQueue.consume();
      this.publish(exchange, routingKey, message, options);
    }
  }

  async close(): Promise<void> {
    try {
      // attempt to flush queued up messages, if any
      this.flush();
      // close channel
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

          await callback(body, message);
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
  // Consume/publish methods
  //

  async consumeQueue<T>(queue: string, callback: ChannelMessageCallback<T>, options?: ConsumeOptions): Promise<string> {
    // build callback
    const { consumerTag } = await this.addConsumer(queue, callback, options);
    // register subscription so resubscription is possible with other connection/channel
    this.subscriptions[consumerTag] = {
      consumerTag,
      source: queue,
      active: true,
      resubscribe: () => this.consumeQueue(queue, callback, options),
    };

    yall.info(`Subscribed to messages from queue ${queue}`);
    return consumerTag;
  }

  async consume<T>(exchange: string, pattern: string, callback: ChannelMessageCallback<T>, options?: ConsumeOptions): Promise<string> {
    // assert temporary queue with given parameters
    const queue = `${exchange}-${id()}`;
    await this.channel.assertQueue(queue, { exclusive: true, autoDelete: true });
    await this.channel.bindQueue(queue, exchange, pattern);
    // build callback
    const { consumerTag } = await this.addConsumer(queue, callback, options);
    // register subscription so resubscription is possible with other connection/channel
    this.subscriptions[consumerTag] = {
      consumerTag,
      source: exchange,
      active: true,
      resubscribe: () => this.consume(exchange, pattern, callback, options),
    };

    yall.info(`Subscribed to exchange ${exchange}:${pattern} via temp queue ${queue}`);
    return consumerTag;
  }

  publish<T>(exchange: string, routingKey: string, message: T, options?: PublishOptions): boolean {
    const body = options?.raw ? (message as unknown as Buffer) : Buffer.from(JSON.stringify(message));

    // if connected, send directly
    if (this.connected) {
      if (!exchange) {
        return this.channel.sendToQueue(routingKey, body, options);
      }
      return this.channel.publish(exchange, routingKey, body, options);
    }

    // disconnected - will try to reconnect, in the meantime queue up message
    this.backedUpQueue.push({ exchange, routingKey, message, options });
    return true;
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

    yall.info(`Unsubscribing from ${this.subscriptions[consumerTag].source}`);
    await this.channel.cancel(consumerTag);
    delete this.subscriptions[consumerTag];
    return true;
  }
}
