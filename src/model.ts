import { Options } from 'amqplib';
import yall from 'yall2';
import id from './util';
import { ChannelMessageCallback, ConsumeOptions, ExclusiveConsumeOptions, PublishOptions } from './api';
import FifoQueue from './queue';
import { ChannelPoolSubscription } from './channelpool';

//
// declarative representations of subscriptions
// and outgoing messages
//

type QueueSubscription<T> = {
  type: 'queue';
  channelPoolSubscription: ChannelPoolSubscription;
  channelName: string;
  source: string;
  active: boolean;
  callback: ChannelMessageCallback<T>;
  options?: ConsumeOptions;
};

type ExchangeSubscription<T> = {
  type: 'exchange';
  channelPoolSubscription: ChannelPoolSubscription;
  source: string;
  pattern: string;
  active: boolean;
  callback: ChannelMessageCallback<T>;
  options: ExclusiveConsumeOptions;
};

type Subscription<T> = QueueSubscription<T> | ExchangeSubscription<T>;

type PublishMessage<T> = {
  exchange?: string;
  routingKey: string;
  message: T;
  options?: PublishOptions;
};

//
// declarative representations of some rabbitmq
// entities we can replay after reconnecting
//

type Binding = {
  destination: string;
  source: string;
  pattern: string;
  args: Record<string, unknown>;
};

type BindingKey = [string, string];

type Queue = {
  name: string;
  asserted: boolean;
  options?: Options.AssertQueue;
  bindings: Map<BindingKey, Binding>;
  subscriptions: Map<string, QueueSubscription<never>>;
};

type Exchange = {
  name: string;
  asserted: boolean;
  type?: string;
  options?: Options.AssertExchange;
  bindings: Map<BindingKey, Binding>;
  subscriptions: Map<string, ExchangeSubscription<never>>;
  backedUpQueue: FifoQueue<PublishMessage<unknown>>;
};

/**
 * Maintainable model for RabbitMQ.
 * Contains programmatic version of queues, exchanges and bindings asserted.
 *
 * Can be re-played after a reconnection, to make sure all idempotent commands are executed again,
 * in case queues are deleted.
 *
 * Subscriptions are maintained which can be re-bound.
 *
 * Unpublished messages during an outage are also queued here.
 */
export default class RabbitModel {
  exchanges: Map<string, Exchange>;
  queues: Map<string, Queue>;
  subscriptions: Map<string, Subscription<never>>;

  constructor() {
    this.exchanges = new Map();
    this.queues = new Map();
    this.subscriptions = new Map();
  }

  //
  // idempotent assertion/deletions
  // imperative modifications to declarative model
  //

  private assertQueueInModel(queue: string): void {
    if (!this.queues.has(queue)) {
      this.queues.set(queue, {
        name: queue,
        asserted: false,
        bindings: new Map(),
        subscriptions: new Map(),
      });
    }
  }

  assertQueue(queue: string, options?: Options.AssertQueue): void {
    this.queues.set(queue, {
      name: queue,
      asserted: true,
      options,
      bindings: new Map(),
      subscriptions: new Map(),
    });
  }

  deleteQueue(queue: string): void {
    this.queues.delete(queue);
  }

  bindQueue(queue: string, source: string, pattern: string, args?: Record<string, unknown>): void {
    this.assertQueueInModel(queue);
    this.queues.get(queue).bindings.set([source, pattern], {
      destination: queue,
      source,
      pattern,
      args,
    });
  }

  unbindQueue(queue: string, source: string, pattern: string): void {
    this.queues.get(queue)?.bindings.delete([source, pattern]);
  }

  private assertExchangeInModel(exchange: string): void {
    if (!this.exchanges.has(exchange)) {
      this.exchanges.set(exchange, {
        name: exchange,
        asserted: false,
        bindings: new Map(),
        subscriptions: new Map(),
        backedUpQueue: new FifoQueue(),
      });
    }
  }

  assertExchange(exchange: string, type: string, options?: Options.AssertExchange): void {
    this.exchanges.set(exchange, {
      name: exchange,
      asserted: true,
      type,
      options,
      bindings: new Map(),
      subscriptions: new Map(),
      backedUpQueue: new FifoQueue(),
    });
  }

  deleteExchange(exchange: string): void {
    this.exchanges.delete(exchange);
  }

  bindExchange(destination: string, source: string, pattern: string, args?: Record<string, unknown>): void {
    this.assertExchangeInModel(destination);
    this.exchanges.get(destination).bindings.set([source, pattern], {
      destination,
      source,
      pattern,
      args,
    });
  }

  unbindExchange(destination: string, source: string, pattern: string): void {
    this.exchanges.get(destination)?.bindings.delete([source, pattern]);
  }

  //
  // adding subscriptions
  //

  consumeQueue<T>(
    channelPoolSubscription: ChannelPoolSubscription,
    queue: string,
    callback: ChannelMessageCallback<T>,
    options?: ConsumeOptions,
  ): string {
    const subscriptionId = id();
    const subscription = {
      type: 'queue',
      active: true,
      channelPoolSubscription,
      source: queue,
      callback,
      options,
    } as QueueSubscription<T>;

    this.assertQueueInModel(queue);
    this.queues.get(queue).subscriptions.set(subscriptionId, subscription);
    this.subscriptions.set(subscriptionId, subscription);
    return subscriptionId;
  }

  consume<T>(
    channelPoolSubscription: ChannelPoolSubscription,
    exchange: string,
    pattern: string,
    callback: ChannelMessageCallback<T>,
    options?: ExclusiveConsumeOptions,
  ): string {
    const subscriptionId = id();
    const subscription = {
      type: 'exchange',
      active: true,
      channelPoolSubscription,
      source: exchange,
      pattern,
      callback,
      options,
    } as ExchangeSubscription<T>;

    this.assertExchangeInModel(exchange);
    this.exchanges.get(exchange).subscriptions.set(subscriptionId, subscription);
    this.subscriptions.set(subscriptionId, subscription);
    return subscriptionId;
  }

  //
  // pausing/resuming subscriptions
  //

  isSubscriptionActive(subscriptionId: string): boolean {
    return this.subscriptions.get(subscriptionId)?.active;
  }

  pauseSubscription(subscriptionId: string): ChannelPoolSubscription {
    if (!this.subscriptions.has(subscriptionId)) {
      yall.warn(`Trying to pause non-existent subscription ${subscriptionId}`);
      return undefined;
    }

    const subscription = this.subscriptions.get(subscriptionId);
    yall.info(`Unsubscribing from ${subscription.source} (ID ${subscriptionId})`);
    subscription.active = false;
    return subscription.channelPoolSubscription;
  }

  resumeSubscription(subscriptionId: string, channelPoolSubscription: ChannelPoolSubscription): void {
    if (!this.subscriptions.has(subscriptionId)) {
      yall.warn(`Trying to resume non-existent subscription ${subscriptionId}`);
      return;
    }

    const subscription = this.subscriptions.get(subscriptionId);
    yall.info(`Resubscribing to ${subscription.source}`);
    subscription.active = true;
    subscription.channelPoolSubscription = channelPoolSubscription;
  }

  cancelSubscription(subscriptionId: string): ChannelPoolSubscription {
    if (!this.subscriptions.has(subscriptionId)) {
      yall.warn(`Trying to cancel non-existent subscription ${subscriptionId}`);
      return undefined;
    }

    const subscription = this.subscriptions.get(subscriptionId);
    const { source, active } = subscription;

    yall.info(`Unsubscribing from ${source}`);

    let channelPoolSubscription: ChannelPoolSubscription;
    if (active) {
      channelPoolSubscription = subscription.channelPoolSubscription;
    } else {
      yall.warn(`Cancelling paused subscription ${source}`);
    }

    this.subscriptions.delete(subscriptionId);
    if (subscription.type === 'queue') {
      this.queues.get(source)?.subscriptions.delete(subscriptionId);
    } else {
      this.exchanges.get(source)?.subscriptions.delete(subscriptionId);
    }

    return channelPoolSubscription;
  }

  //
  // publishing
  // not idempotent, therefore should only be called when direct publishing fails
  //

  publish<T>(exchange: string, routingKey: string, message: T, options?: PublishOptions): void {
    this.assertExchangeInModel(exchange);
    this.exchanges.get(exchange).backedUpQueue.push({ exchange, routingKey, message, options });
  }
}
