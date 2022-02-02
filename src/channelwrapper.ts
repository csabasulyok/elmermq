import { Connection, Channel, ConsumeMessage, Options, Replies } from 'amqplib';
import yall from 'yall2';
import autoBind from 'auto-bind';
import { ConsumeOptions, ChannelMessageCallback, PublishOptions, ExclusiveConsumeOptions } from './api';
import id from './util';

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

  async connect(connection: Connection): Promise<void> {
    this.channel = await connection.createChannel();
    this.channel.prefetch(1);
  }

  async close(): Promise<void> {
    try {
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
    yall.info(`Subscribed to messages from queue ${queue}`);
    return consumerTag;
  }

  async consume<T>(
    exchange: string,
    pattern: string,
    callback: ChannelMessageCallback<T>,
    options?: ExclusiveConsumeOptions,
  ): Promise<string> {
    // assert temporary queue with given parameters
    const queue = `${options?.queuePrefix || exchange}-${id()}`;
    await this.channel.assertQueue(queue, { exclusive: true, autoDelete: true });
    await this.channel.bindQueue(queue, exchange, pattern);
    // build callback
    const { consumerTag } = await this.addConsumer(queue, callback, options);

    yall.info(`Subscribed to exchange ${exchange}${pattern ? `:${pattern}` : ''} via temp queue ${queue}`);
    return consumerTag;
  }

  publish<T>(exchange: string, routingKey: string, message: T, options?: PublishOptions): boolean {
    const body = options?.raw ? (message as unknown as Buffer) : Buffer.from(JSON.stringify(message));

    if (!exchange) {
      return this.channel.sendToQueue(routingKey, body, options);
    }
    return this.channel.publish(exchange, routingKey, body, options);
  }

  //
  // Pausing/resuming
  //

  async cancel(consumerTag: string): Promise<void> {
    await this.channel.cancel(consumerTag);
  }
}
