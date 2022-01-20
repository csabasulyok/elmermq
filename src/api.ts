import { Options, Replies, MessageProperties } from 'amqplib';
import extol, { extolPrefix, WithExtolProps } from 'extol';

/**
 * Extended version of amqplib connect options.
 */
export interface ConnectOptions extends Options.Connect {
  /**
   * Interval in milliseconds to try to reconnect and resubscribe
   * if the connection is lost.
   * Default: 5000 (5 seconds)
   */
  reconnectInterval?: number;

  /**
   * Number of attempts to make to reconnect to broker,
   * after which we exit with an error.
   * Default: 10
   */
  reconnectNumAttempts?: number;

  /**
   * Channel pool size.
   * Default: 1
   */
  poolSize?: number;

  /**
   * Connection label to show on RabbitMQ management device
   * Default: elmermq
   */
  connectionLabel?: string;
}

/**
 * Default instance of connect options.
 * Uses extol to read .env/environment variable information
 * Environment variables are prefixed, e.g. ELMERMQ_POOL_SIZE, ELMERMQ_PROTOCOL
 */
@extolPrefix('elmermq')
class ExtolConnectOptions extends WithExtolProps<ConnectOptions> {
  @extol(5000)
  reconnectInterval: number;

  @extol(10)
  reconnectNumAttempts: number;

  @extol(1)
  poolSize: number;

  @extol('elmermq')
  connectionLabel: string;

  @extol('amqp')
  protocol: string;

  @extol('localhost')
  hostname: string;

  @extol(5672)
  port: number;

  @extol('guest')
  username: string;

  @extol('guest')
  password: string;
}

// default instance exported
export const extolConnectOptions = new ExtolConnectOptions().extolProps();

//
// Callback types
//
export type ConnectCallback = () => void;
export type ErrorCallback = (message: string) => void;
export type CloseCallback = () => void;
// eslint-disable-next-line no-use-before-define
export type MessageCallback<T> = (message: T, connection: ElmerConnection, properties?: MessageProperties) => Promise<void> | void;
export type ChannelMessageCallback<T> = (message: T, properties?: MessageProperties) => Promise<void> | void;

//
// Callback options
//

export type ConsumeOptions = Options.Consume & {
  /**
   * If set, a message triggers a callback with a Buffer (the original method).
   * Otherwise, the buffer is attempted to be parsed as JSON
   * Default: false
   */
  raw?: boolean;
};

export type PublishOptions = Options.Publish & {
  /**
   * If set, the input must be a Buffer to be sent directly.
   * Otherwise, the buffer is attempted to be stringified as JSON
   * Default: false
   */
  raw?: boolean;
};

/**
 * Return key so that client can refer to a subscription of certain channel
 */
export type ChannelPoolSubscription = {
  channelName: string;
  consumerTag: string;
};

//
// Message
/**
 * A connection to the AMQP server.
 * Uses auto-reconnect and channel pooling.
 */
export default interface ElmerConnection {
  //
  // Connection management
  //

  connect(): Promise<void>;
  close(): Promise<void>;

  //
  // Callbacks for life cycle events
  //

  onConnect(callback: ConnectCallback): void;
  onError(callback: ErrorCallback): void;
  onClose(callback: CloseCallback): void;

  //
  // Decorated methods of channel from channel pool
  //

  assertQueue(queue: string, options?: Options.AssertQueue): Promise<Replies.AssertQueue>;
  deleteQueue(queue: string, options?: Options.DeleteQueue): Promise<Replies.DeleteQueue>;

  bindQueue(queue: string, source: string, pattern: string, args?: Record<string, unknown>): Promise<Replies.Empty>;
  unbindQueue(queue: string, source: string, pattern: string, args?: Record<string, unknown>): Promise<Replies.Empty>;

  assertExchange(exchange: string, type: string, options?: Options.AssertExchange): Promise<Replies.AssertExchange>;
  deleteExchange(exchange: string, options?: Options.DeleteExchange): Promise<Replies.Empty>;

  bindExchange(destination: string, source: string, pattern: string, args?: Record<string, unknown>): Promise<Replies.Empty>;
  unbindExchange(destination: string, source: string, pattern: string, args?: Record<string, unknown>): Promise<Replies.Empty>;

  consumeQueue<T>(queue: string, callback: MessageCallback<T>, options?: ConsumeOptions): Promise<ChannelPoolSubscription>;
  consume<T>(exchange: string, pattern: string, callback: MessageCallback<T>, options?: ConsumeOptions): Promise<ChannelPoolSubscription>;
  sendToQueue<T>(queue: string, message: T, options?: PublishOptions): boolean;
  publish<T>(exchange: string, routingKey: string, message: T, options?: PublishOptions): boolean;

  //
  // Pausing/resuming
  //

  isListenerActive(subscription: ChannelPoolSubscription): boolean;
  pauseListener(subscription: ChannelPoolSubscription): Promise<boolean>;
  resumeListener(subscription: ChannelPoolSubscription): Promise<ChannelPoolSubscription>;
  stopListener(subscription: ChannelPoolSubscription): Promise<boolean>;
}
