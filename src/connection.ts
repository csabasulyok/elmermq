import amqp, { Connection, Options, Replies, MessageProperties } from 'amqplib';
import yall from 'yall2';
import autoBind from 'auto-bind';

import ElmerConnection, {
  ChannelMessageCallback,
  ChannelPoolSubscription,
  CloseCallback,
  ConnectCallback,
  ConnectOptions,
  ErrorCallback,
  extolConnectOptions,
  MessageCallback,
  OnExchangeOptions,
  OnQueueOptions,
  PublishOptions,
} from './api';
import { MessageBrokerSubscription } from './subscription';
import ChannelPool from './channelpool';

export default class ElmerConnectionImpl implements ElmerConnection {
  // connection options
  private connectOptions: ConnectOptions;
  private socketOptions: Record<string, unknown>;
  // amqp objects
  private connection: Connection;
  private channelPool: ChannelPool;
  private closeRequested: boolean;
  // reconnection settings
  private timeout?: NodeJS.Timeout;
  private reconnectAttempts = 0;

  // dynamic subscriptions, temp queues
  private subscriptions: {
    [objectName: string]: {
      [consumerKey: string]: MessageBrokerSubscription;
    };
  };

  // callbacks for proprietary/wrapped events
  callbacks: {
    onConnect?: ConnectCallback;
    onError?: ErrorCallback;
    onClose?: CloseCallback;
  };

  constructor(connectOptions?: string | ConnectOptions, socketOptions?: Record<string, unknown>) {
    let customConnectOptions: ConnectOptions;

    if (typeof connectOptions === 'string' || connectOptions instanceof String) {
      const url = new URL(connectOptions as string);
      customConnectOptions = {
        protocol: url.protocol || undefined,
        hostname: url.hostname || undefined,
        port: Number(url.port) || undefined,
        username: url.username || undefined,
        password: url.password || undefined,
        vhost: url.pathname || undefined,
      };
    } else {
      customConnectOptions = connectOptions;
    }

    this.connectOptions = {
      ...extolConnectOptions,
      ...customConnectOptions,
    };

    this.socketOptions = {
      ...socketOptions,
      clientProperties: {
        ...(socketOptions?.clientProperties as Record<string, unknown>),
        connection_name: this.connectOptions.connectionLabel,
      },
    };

    this.callbacks = {};
    this.channelPool = new ChannelPool(this.connectOptions.poolSize);
    this.closeRequested = false;

    autoBind(this);
  }

  async connect(): Promise<void> {
    this.timeout = null;
    const { protocol, hostname, port, reconnectInterval, reconnectNumAttempts } = this.connectOptions;

    try {
      this.reconnectAttempts += 1;
      yall.info(`Connecting to AMQP broker on ${protocol}://${hostname}:${port} (attempt ${this.reconnectAttempts})...`);
      this.connection = await amqp.connect(this.connectOptions, this.socketOptions);
    } catch (e) {
      if (!this.timeout) {
        yall.error(`Connection failed, attempting again in ${reconnectInterval}ms...`);
        this.timeout = setTimeout(() => this.connect(), reconnectInterval);
      }
      if (this.reconnectAttempts >= reconnectNumAttempts) {
        yall.error(`Could not reconnect in ${reconnectNumAttempts} tries, aborting...`);
        this.callbacks.onError?.(`Could not reconnect in ${reconnectNumAttempts} tries`);
      }
      return;
    }

    this.connection.on('error', (err) => {
      // if connection is closed, we can try to re-connect
      if (err.message !== 'Connection closing') {
        yall.error(`Connection error: ${err.message}`);
      } else {
        this.callbacks.onError?.(err.message);
      }
    });

    this.connection.on('close', () => {
      if (this.closeRequested) {
        yall.info('AMQP broker connection closed successfully');
        return;
      }
      if (!this.timeout) {
        yall.error('Connection closed, reconnecting...');
        this.timeout = setTimeout(() => this.connect(), reconnectInterval);
      }
      if (this.reconnectAttempts >= reconnectNumAttempts) {
        yall.warn(`Could not reconnect in ${reconnectNumAttempts} tries, aborting...`);
        this.callbacks.onError?.(`Could not reconnect in ${reconnectNumAttempts} tries`);
      }
    });

    this.reconnectAttempts = 0;

    await this.channelPool.connect(this.connection);

    this.subscriptions = {};

    // if this is a reconnection, reconnect all previously active subscriptions
    Object.values(this.subscriptions).forEach((subscriptions) => {
      Object.values(subscriptions).forEach((subscription) => {
        if (subscription?.active) {
          subscription?.resubscribe();
        }
      });
    });

    // close connection on Ctrl+C
    process.once('SIGINT', this.close);
    process.once('SIGTERM', this.close);

    this.callbacks.onConnect?.();
  }

  async close(): Promise<void> {
    this.closeRequested = true;
    await this.channelPool?.close();
    await this.connection?.close();
    this.callbacks.onClose?.();
  }

  //
  // Public callback assignments for client
  //

  onConnect(callback: ConnectCallback): void {
    this.callbacks.onConnect = callback;
  }

  onError(callback: ErrorCallback): void {
    this.callbacks.onError = callback;
  }

  onClose(callback: CloseCallback): void {
    this.callbacks.onClose = callback;
  }

  //
  // Channel decorators
  //

  async assertQueue(queue: string, options?: Options.AssertQueue): Promise<Replies.AssertQueue> {
    const ret = await this.channelPool.assertQueue(queue, options);
    return ret;
  }

  async deleteQueue(queue: string, options?: Options.DeleteQueue): Promise<Replies.DeleteQueue> {
    const ret = await this.channelPool.deleteQueue(queue, options);
    return ret;
  }

  async bindQueue(queue: string, source: string, pattern: string, args?: Record<string, unknown>): Promise<Replies.Empty> {
    const ret = await this.channelPool.bindQueue(queue, source, pattern, args);
    return ret;
  }

  async unbindQueue(queue: string, source: string, pattern: string, args?: Record<string, unknown>): Promise<Replies.Empty> {
    const ret = await this.channelPool.unbindQueue(queue, source, pattern, args);
    return ret;
  }

  async assertExchange(exchange: string, type: string, options?: Options.AssertExchange): Promise<Replies.AssertExchange> {
    const ret = await this.channelPool.assertExchange(exchange, type, options);
    return ret;
  }

  async deleteExchange(exchange: string, options?: Options.DeleteExchange): Promise<Replies.Empty> {
    const ret = await this.channelPool.deleteExchange(exchange, options);
    return ret;
  }

  async bindExchange(destination: string, source: string, pattern: string, args?: Record<string, unknown>): Promise<Replies.Empty> {
    const ret = await this.channelPool.bindExchange(destination, source, pattern, args);
    return ret;
  }

  async unbindExchange(destination: string, source: string, pattern: string, args?: Record<string, unknown>): Promise<Replies.Empty> {
    const ret = await this.channelPool.unbindExchange(destination, source, pattern, args);
    return ret;
  }

  //
  // Message handling
  //

  async onQueueMessage<T>(queue: string, callback: MessageCallback<T>, options?: OnQueueOptions): Promise<ChannelPoolSubscription> {
    const decoratedCallback: ChannelMessageCallback<T> = (message: T, properties?: MessageProperties) =>
      callback(message, this, properties);
    const ret = await this.channelPool.onQueueMessage(queue, decoratedCallback, options);
    return ret;
  }

  async onFanoutMessage<T>(fanout: string, callback: MessageCallback<T>, options?: OnExchangeOptions): Promise<ChannelPoolSubscription> {
    const decoratedCallback: ChannelMessageCallback<T> = (message: T, properties?: MessageProperties) =>
      callback(message, this, properties);
    const ret = await this.channelPool.onFanoutMessage(fanout, decoratedCallback, options);
    return ret;
  }

  async onTopicMessage<T>(
    topic: string,
    pattern: string,
    callback: MessageCallback<T>,
    options?: OnExchangeOptions,
  ): Promise<ChannelPoolSubscription> {
    const decoratedCallback: ChannelMessageCallback<T> = (message: T, properties?: MessageProperties) =>
      callback(message, this, properties);
    const ret = await this.channelPool.onTopicMessage(topic, pattern, decoratedCallback, options);
    return ret;
  }

  publishToQueue<T>(queue: string, message: T, options?: PublishOptions): boolean {
    return this.channelPool.publishToQueue(queue, message, options);
  }

  publishToDirect<T>(direct: string, routingKey: string, message: T, options?: PublishOptions): boolean {
    return this.channelPool.publishToDirect(direct, routingKey, message, options);
  }

  publishToFanout<T>(fanout: string, message: T, options?: PublishOptions): boolean {
    return this.channelPool.publishToFanout(fanout, message, options);
  }

  publishToTopic<T>(topic: string, routingKey: string, message: T, options?: PublishOptions): boolean {
    return this.channelPool.publishToTopic(topic, routingKey, message, options);
  }

  //
  // Pausing/resuming
  //

  isListenerActive(subscription: ChannelPoolSubscription): boolean {
    return this.channelPool.isListenerActive(subscription);
  }

  async pauseListener(subscription: ChannelPoolSubscription): Promise<boolean> {
    const ret = await this.channelPool.pauseListener(subscription);
    return ret;
  }

  async resumeListener(subscription: ChannelPoolSubscription): Promise<ChannelPoolSubscription> {
    const ret = await this.channelPool.resumeListener(subscription);
    return ret;
  }

  async stopListener(subscription: ChannelPoolSubscription): Promise<boolean> {
    const ret = await this.channelPool.stopListener(subscription);
    return ret;
  }
}
