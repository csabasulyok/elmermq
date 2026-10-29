import { ChannelModel, Channel, ConsumeMessage, Options, Replies } from 'amqplib';
import yall from 'yall2';
import autoBind from 'auto-bind';
import {
  ConsumeOptions,
  ChannelMessageCallback,
  PublishOptions,
  ExclusiveConsumeOptions,
  RequeueError,
} from './api.js';
import id from './util.js';

/**
 * Wrapper around AMQP channel
 * with convenience methods
 */
export default class ChannelWrapper {
  private channel: Channel;

  name: string;

  constructor(name: string) {
    this.name = name;
    autoBind(this);
  }

  async connect(connection: ChannelModel): Promise<void> {
    this.channel = await connection.createChannel();
    await this.channel.prefetch(1);
  }

  async close(): Promise<void> {
    try {
      // close channel
      await this.channel.close();
    } catch (error) {
      // channel already closing because the parent connection is, so it can be ignored
      if ((error as Error | undefined)?.message !== 'Channel closing') {
        throw error;
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

  async bindQueue(
    queue: string,
    source: string,
    pattern: string,
    args?: Record<string, unknown>,
  ): Promise<Replies.Empty> {
    const ret = await this.channel.bindQueue(queue, source, pattern, args);
    return ret;
  }

  async unbindQueue(
    queue: string,
    source: string,
    pattern: string,
    args?: Record<string, unknown>,
  ): Promise<Replies.Empty> {
    const ret = await this.channel.unbindQueue(queue, source, pattern, args);
    return ret;
  }

  async assertExchange(
    exchange: string,
    type: string,
    options?: Options.AssertExchange,
  ): Promise<Replies.AssertExchange> {
    const ret = await this.channel.assertExchange(exchange, type, options);
    return ret;
  }

  async deleteExchange(exchange: string, options?: Options.DeleteExchange): Promise<Replies.Empty> {
    const ret = await this.channel.deleteExchange(exchange, options);
    return ret;
  }

  async bindExchange(
    destination: string,
    source: string,
    pattern: string,
    args?: Record<string, unknown>,
  ): Promise<Replies.Empty> {
    const ret = await this.channel.bindExchange(destination, source, pattern, args);
    return ret;
  }

  async unbindExchange(
    destination: string,
    source: string,
    pattern: string,
    args?: Record<string, unknown>,
  ): Promise<Replies.Empty> {
    const ret = await this.channel.unbindExchange(destination, source, pattern, args);
    return ret;
  }

  //
  // Common convenience method for processing a message
  //

  private async addConsumer<T>(
    queue: string,
    callback: ChannelMessageCallback<T>,
    options?: ConsumeOptions,
  ): Promise<Replies.Consume> {
    const response = await this.channel.consume(
      queue,
      async (message: ConsumeMessage | null) => {
        if (!message) {
          return;
        }

        try {
          yall.debug(`[${queue}] consuming message of length ${message.content.length}`);

          // if raw, pass buffer as is, otherwise deserialize JSON
          const body = options?.raw ? (message.content as unknown as T) : (JSON.parse(message.content.toString()) as T);
          yall.debug(body);

          await callback(body, message);
          this.channel.ack(message);
        } catch (error) {
          if (error instanceof RequeueError) {
            yall.warn(`[${queue}] requeueing message, ${error.message}`);
            this.channel.reject(message, true);
            return;
          }
          yall.error(`[${queue}] error, ${error}`);
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
    yall.info(`Subscribed to messages from queue ${queue}`);
    return consumerTag;
  }

  async consume<T>(
    exchange: string,
    pattern: string,
    callback: ChannelMessageCallback<T>,
    options?: ExclusiveConsumeOptions,
  ): Promise<string> {
    let queue: string;
    if (options?.queueName) {
      // named queue, outlives its consumers until it expires
      queue = options.queueName;
      await this.channel.assertQueue(queue, {
        durable: false,
        exclusive: false,
        autoDelete: false,
        ...options.queueOptions,
      });
    } else {
      // assert temporary queue with given parameters
      // an empty prefix also falls back to the exchange name
      // eslint-disable-next-line @typescript-eslint/prefer-nullish-coalescing
      queue = `${options?.queuePrefix || exchange}-${id()}`;
      await this.channel.assertQueue(queue, { exclusive: true, autoDelete: true });
    }
    await this.channel.bindQueue(queue, exchange, pattern);
    // build callback
    const { consumerTag } = await this.addConsumer(queue, callback, options);

    yall.info(`Subscribed to exchange ${exchange}${pattern ? `:${pattern}` : ''} via queue ${queue}`);
    return consumerTag;
  }

  publish<T>(exchange: string, routingKey: string, message: T, options?: PublishOptions): boolean {
    const body = options?.raw ? (message as unknown as Buffer) : Buffer.from(JSON.stringify(message));

    return !exchange
      ? this.channel.sendToQueue(routingKey, body, options)
      : this.channel.publish(exchange, routingKey, body, options);
  }

  //
  // Pausing/resuming
  //

  async cancel(consumerTag: string): Promise<void> {
    await this.channel.cancel(consumerTag);
  }
}
