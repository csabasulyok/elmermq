/* eslint-disable no-restricted-syntax */
/* eslint-disable no-await-in-loop */
import amqp, { Connection, Options, Replies, ConsumeMessage } from 'amqplib';
import yall from 'yall2';
import autoBind from 'auto-bind';

import ElmerConnection, {
  ChannelMessageCallback,
  ConnectOptions,
  ConsumeOptions,
  ErrorCallback,
  ExclusiveConsumeOptions,
  extolConnectOptions,
  MessageCallback,
  PublishOptions,
} from './api';
import ChannelPool, { ChannelPoolSubscription } from './channelpool';
import RabbitModel from './model';

export default class ElmerConnectionImpl implements ElmerConnection {
  // connection options
  private connectOptions: ConnectOptions;
  private socketOptions: Record<string, unknown>;
  // amqp objects
  private connection: Connection;
  private channelPool: ChannelPool;
  private model: RabbitModel;
  private closeRequested: boolean;
  // reconnection settings
  private timeout?: NodeJS.Timeout;
  private reconnectAttempts = 0;
  // replayable actions
  private connected: boolean;

  // callbacks for proprietary/wrapped events
  callbacks: {
    onError?: ErrorCallback;
  };

  constructor(urlOrConnectOptions?: string | ConnectOptions, socketOptions?: Record<string, unknown>) {
    let customConnectOptions: ConnectOptions;

    if (typeof urlOrConnectOptions === 'string' || urlOrConnectOptions instanceof String) {
      const url = new URL(urlOrConnectOptions as string);
      customConnectOptions = {
        protocol: url.protocol || undefined,
        hostname: url.hostname || undefined,
        port: Number(url.port) || undefined,
        username: url.username || undefined,
        password: url.password || undefined,
        vhost: url.pathname || undefined,
      };
    } else {
      customConnectOptions = urlOrConnectOptions;
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

    // default onerror callback - exit process!
    this.callbacks = {
      onError: (message: string) => {
        yall.error(`RabbitMQ connection error: ${message}. Exiting process. Associate callback to override this`);
        process.exit(1);
      },
    };

    this.channelPool = new ChannelPool(this.connectOptions.poolSize);
    this.model = new RabbitModel();
    this.closeRequested = false;
    this.connected = false;

    autoBind(this);
  }

  async connect(failOnError = true): Promise<void> {
    this.timeout = null;
    const { protocol, hostname, port, reconnectInterval, reconnectNumAttempts } = this.connectOptions;

    try {
      this.reconnectAttempts += 1;
      const url = `${protocol}://${hostname}:${port}`;
      yall.info(`Connecting to AMQP broker on ${url} (attempt ${this.reconnectAttempts})...`);
      this.connection = await amqp.connect(this.connectOptions, this.socketOptions);
      this.connected = true;
    } catch (e) {
      if (!this.timeout && !failOnError) {
        yall.error(`Connection failed, attempting again in ${reconnectInterval}ms...`);
        this.timeout = setTimeout(() => this.connect(false), reconnectInterval);
      }
      if (this.reconnectAttempts >= reconnectNumAttempts) {
        yall.error(`Could not reconnect in ${reconnectNumAttempts} tries, aborting...`);
        this.callbacks.onError?.(`Could not reconnect in ${reconnectNumAttempts} tries`);
      }
      if (failOnError) {
        yall.error('Could not connect');
        this.callbacks.onError?.(`Could not start first AMQP connection to ${protocol}://${hostname}:${port}`);
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
      this.connected = false;
      if (this.closeRequested) {
        yall.info('AMQP broker connection closed successfully');
        return;
      }
      if (!this.timeout) {
        yall.error('Connection closed, reconnecting...');
        this.timeout = setTimeout(() => this.connect(false), reconnectInterval);
      }
      if (this.reconnectAttempts >= reconnectNumAttempts) {
        yall.warn(`Could not reconnect in ${reconnectNumAttempts} tries, aborting...`);
        this.callbacks.onError?.(`Could not reconnect in ${reconnectNumAttempts} tries`);
      }
    });

    this.reconnectAttempts = 0;

    await this.channelPool.connect(this.connection);

    // replay model
    await this.replayModel();
  }

  async close(): Promise<void> {
    this.closeRequested = true;
    await this.channelPool?.close();
    if (this.connected) {
      await this.connection?.close();
    }
  }

  private async replayModel(): Promise<void> {
    // assert exchanges
    for (const exchange of this.model.exchanges.values()) {
      if (exchange.asserted) {
        await this.channelPool.assertExchange(exchange.name, exchange.type, exchange.options);
      }
    }
    // assert queues
    for (const queue of this.model.queues.values()) {
      if (queue.asserted) {
        await this.channelPool.assertQueue(queue.name, queue.options);
      }
    }
    // exchange-to-exchange bindings
    for (const exchange of this.model.exchanges.values()) {
      for (const { source, pattern, args } of exchange.bindings.values()) {
        await this.channelPool.bindExchange(exchange.name, source, pattern, args);
      }
    }
    // queue-to-exchange bindings
    for (const queue of this.model.queues.values()) {
      for (const { source, pattern, args } of queue.bindings.values()) {
        await this.channelPool.bindQueue(queue.name, source, pattern, args);
      }
    }

    // resume subscriptions
    await this.resumeAllSubscriptions();

    // flush messages that couldn't be sent
    for (const exchange of this.model.exchanges.values()) {
      while (exchange.backedUpQueue.backedUp) {
        const { routingKey, message, options } = exchange.backedUpQueue.consume();
        this.publish(exchange.name, routingKey, message, options);
      }
    }
  }

  //
  // Public callback assignments for client
  //

  onError(callback: ErrorCallback): void {
    this.callbacks.onError = callback;
  }

  //
  // Channel decorators
  //

  async assertQueue(queue: string, options?: Options.AssertQueue): Promise<Replies.AssertQueue> {
    this.model.assertQueue(queue, options);
    const ret = await this.channelPool.assertQueue(queue, options);
    return ret;
  }

  async deleteQueue(queue: string, options?: Options.DeleteQueue): Promise<Replies.DeleteQueue> {
    this.model.deleteQueue(queue);
    const ret = await this.channelPool.deleteQueue(queue, options);
    return ret;
  }

  async bindQueue(
    queue: string,
    source: string,
    pattern: string,
    args?: Record<string, unknown>,
  ): Promise<Replies.Empty> {
    this.model.bindQueue(queue, source, pattern, args);
    const ret = await this.channelPool.bindQueue(queue, source, pattern, args);
    return ret;
  }

  async unbindQueue(
    queue: string,
    source: string,
    pattern: string,
    args?: Record<string, unknown>,
  ): Promise<Replies.Empty> {
    this.model.unbindQueue(queue, source, pattern);
    const ret = await this.channelPool.unbindQueue(queue, source, pattern, args);
    return ret;
  }

  async assertExchange(
    exchange: string,
    type: string,
    options?: Options.AssertExchange,
  ): Promise<Replies.AssertExchange> {
    this.model.assertExchange(exchange, type, options);
    const ret = await this.channelPool.assertExchange(exchange, type, options);
    return ret;
  }

  async deleteExchange(exchange: string, options?: Options.DeleteExchange): Promise<Replies.Empty> {
    this.model.deleteExchange(exchange);
    const ret = await this.channelPool.deleteExchange(exchange, options);
    return ret;
  }

  async bindExchange(
    destination: string,
    source: string,
    pattern: string,
    args?: Record<string, unknown>,
  ): Promise<Replies.Empty> {
    this.model.bindExchange(destination, source, pattern, args);
    const ret = await this.channelPool.bindExchange(destination, source, pattern, args);
    return ret;
  }

  async unbindExchange(
    destination: string,
    source: string,
    pattern: string,
    args?: Record<string, unknown>,
  ): Promise<Replies.Empty> {
    this.model.unbindExchange(destination, source, pattern);
    const ret = await this.channelPool.unbindExchange(destination, source, pattern, args);
    return ret;
  }

  //
  // consuming messages
  //

  async consumeQueue<T>(queue: string, callback: MessageCallback<T>, options?: ConsumeOptions): Promise<string> {
    const decoratedCallback: ChannelMessageCallback<T> = (message: T, rawMessage?: ConsumeMessage) =>
      callback(message, this, rawMessage);
    const channelPoolSubscription = await this.channelPool.consumeQueue(queue, decoratedCallback, options);
    const subscriptionId = this.model.consumeQueue(channelPoolSubscription, queue, decoratedCallback, options);
    return subscriptionId;
  }

  async consume<T>(
    exchange: string,
    pattern: string,
    callback: MessageCallback<T>,
    options?: ExclusiveConsumeOptions,
  ): Promise<string> {
    const decoratedCallback: ChannelMessageCallback<T> = (message: T, rawMessage?: ConsumeMessage) =>
      callback(message, this, rawMessage);
    const channelPoolSubscription = await this.channelPool.consume(exchange, pattern, decoratedCallback, options);
    const subscriptionId = this.model.consume(channelPoolSubscription, exchange, pattern, decoratedCallback, options);
    return subscriptionId;
  }

  //
  // publishing messages
  //

  sendToQueue<T>(queue: string, message: T, options?: PublishOptions): boolean {
    return this.publish('', queue, message, options);
  }

  publish<T>(exchange: string, routingKey: string, message: T, options?: PublishOptions): boolean {
    // if connected, send directly
    if (this.connected) {
      return this.channelPool.publish(exchange, routingKey, message, options);
    }
    // disconnected - will try to reconnect, in the meantime queue up message
    this.model.publish(exchange, routingKey, message, options);
    return true;
  }

  //
  // Pausing/resuming
  //

  isSubscriptionActive(subscriptionId: string): boolean {
    return this.model.isSubscriptionActive(subscriptionId);
  }

  async pauseAllSubscriptions(): Promise<void> {
    for (const subscriptionId of this.model.subscriptions.keys()) {
      await this.pauseSubscription(subscriptionId);
    }
  }

  async pauseSubscription(subscriptionId: string): Promise<void> {
    const channelPoolSubscription = this.model.pauseSubscription(subscriptionId);
    if (channelPoolSubscription && this.connected) {
      await this.channelPool.cancel(channelPoolSubscription);
    }
  }

  async resumeAllSubscriptions(): Promise<void> {
    for (const subscriptionId of this.model.subscriptions.keys()) {
      await this.resumeSubscription(subscriptionId);
    }
  }

  async resumeSubscription(subscriptionId: string): Promise<void> {
    const subscription = this.model.subscriptions.get(subscriptionId);
    if (subscription && this.connected) {
      let channelPoolSubscription: ChannelPoolSubscription;
      if (subscription.type === 'queue') {
        const { source, callback, options } = subscription;
        channelPoolSubscription = await this.channelPool.consumeQueue(source, callback, options);
      } else {
        const { source, pattern, callback, options } = subscription;
        channelPoolSubscription = await this.channelPool.consume(source, pattern, callback, options);
      }
      this.model.resumeSubscription(subscriptionId, channelPoolSubscription);
    }
  }

  async cancelAllSubscriptions(): Promise<void> {
    for (const subscriptionId of this.model.subscriptions.keys()) {
      await this.cancelSubscription(subscriptionId);
    }
  }

  async cancelSubscription(subscriptionId: string): Promise<void> {
    const channelPoolSubscription = this.model.cancelSubscription(subscriptionId);
    if (channelPoolSubscription && this.connected) {
      await this.channelPool.cancel(channelPoolSubscription);
    }
  }
}
