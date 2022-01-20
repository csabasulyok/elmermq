import { Connection, Options, Replies } from 'amqplib';
import yall from 'yall2';
import autoBind from 'auto-bind';
import ChannelWithRetention from './channelwrapper';
import id from './util';
import { ChannelMessageCallback, ChannelPoolSubscription, ConsumeOptions, PublishOptions } from './api';

/**
 * Round-robin channel pool for an AMQP connection
 */
export default class ChannelPool {
  private connection: Connection;
  private poolSize: number;
  private channelNames: string[];
  private channels: Record<string, ChannelWithRetention>; // round-robin channel pool
  private channelCounter: number;

  constructor(poolSize: number) {
    this.poolSize = poolSize;
    this.channelNames = Array.from({ length: this.poolSize }, id);
    this.channels = Object.fromEntries(this.channelNames.map((name) => [name, new ChannelWithRetention(name)]));
    autoBind(this);
  }

  async connect(connection: Connection): Promise<void> {
    this.connection = connection;
    this.channelCounter = 0;
    await Promise.all(this.channelNames.map((name) => this.channels[name].connect(this.connection)));
    yall.info(`AMQP channel pool initialized with ${this.poolSize} channels`);
  }

  async close(): Promise<void> {
    // close all channels in pool
    await Promise.all(Object.values(this.channels).map((channel) => channel?.close()));
    yall.info('AMQP channel pool shut down');
  }

  private getChannel(): ChannelWithRetention {
    // hand out channel by round robin
    const name = this.channelNames[this.channelCounter];
    this.channelCounter = (this.channelCounter + 1) % this.poolSize;
    return this.channels[name];
  }

  //
  // Proxying simple methods
  //

  async assertQueue(queue: string, options?: Options.AssertQueue): Promise<Replies.AssertQueue> {
    const ret = await this.getChannel().assertQueue(queue, options);
    return ret;
  }

  async deleteQueue(queue: string, options?: Options.DeleteQueue): Promise<Replies.DeleteQueue> {
    const ret = await this.getChannel().deleteQueue(queue, options);
    return ret;
  }

  async bindQueue(queue: string, source: string, pattern: string, args?: Record<string, unknown>): Promise<Replies.Empty> {
    const ret = await this.getChannel().bindQueue(queue, source, pattern, args);
    return ret;
  }

  async unbindQueue(queue: string, source: string, pattern: string, args?: Record<string, unknown>): Promise<Replies.Empty> {
    const ret = await this.getChannel().unbindQueue(queue, source, pattern, args);
    return ret;
  }

  async assertExchange(exchange: string, type: string, options?: Options.AssertExchange): Promise<Replies.AssertExchange> {
    const ret = await this.getChannel().assertExchange(exchange, type, options);
    return ret;
  }

  async deleteExchange(exchange: string, options?: Options.DeleteExchange): Promise<Replies.Empty> {
    const ret = await this.getChannel().deleteExchange(exchange, options);
    return ret;
  }

  async bindExchange(destination: string, source: string, pattern: string, args?: Record<string, unknown>): Promise<Replies.Empty> {
    const ret = await this.getChannel().bindExchange(destination, source, pattern, args);
    return ret;
  }

  async unbindExchange(destination: string, source: string, pattern: string, args?: Record<string, unknown>): Promise<Replies.Empty> {
    const ret = await this.getChannel().unbindExchange(destination, source, pattern, args);
    return ret;
  }

  async consumeQueue<T>(queue: string, callback: ChannelMessageCallback<T>, options?: ConsumeOptions): Promise<ChannelPoolSubscription> {
    const channel = this.getChannel();
    const consumerTag = await channel.consumeQueue(queue, callback, options);
    return { channelName: channel.name, consumerTag };
  }

  async consume<T>(
    exchange: string,
    pattern: string,
    callback: ChannelMessageCallback<T>,
    options?: ConsumeOptions,
  ): Promise<ChannelPoolSubscription> {
    const channel = this.getChannel();
    const consumerTag = await channel.consume(exchange, pattern, callback, options);
    return { channelName: channel.name, consumerTag };
  }

  publish<T>(exchange: string, routingKey: string, message: T, options?: PublishOptions): boolean {
    return this.getChannel().publish(exchange, routingKey, message, options);
  }

  //
  // Pausing/resuming
  //

  isListenerActive(subscription: ChannelPoolSubscription): boolean {
    const channel = this.channels[subscription.channelName];
    return channel?.isListenerActive(subscription.consumerTag);
  }

  async pauseListener(subscription: ChannelPoolSubscription): Promise<boolean> {
    const channel = this.channels[subscription.channelName];
    const ret = await channel?.pauseListener(subscription.consumerTag);
    return ret;
  }

  async resumeListener(subscription: ChannelPoolSubscription): Promise<ChannelPoolSubscription> {
    const channel = this.channels[subscription.channelName];
    const newConsumerTag = await channel?.resumeListener(subscription.consumerTag);
    return {
      channelName: subscription.channelName,
      consumerTag: newConsumerTag,
    };
  }

  async stopListener(subscription: ChannelPoolSubscription): Promise<boolean> {
    const channel = this.channels[subscription.channelName];
    const ret = await channel?.stopListener(subscription.consumerTag);
    return ret;
  }
}
