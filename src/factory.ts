import ElmerConnection, { ConnectOptions } from './api.js';
import ElmerConnectionImpl from './connection.js';

/**
 * Build AMQP connection with channel pooling and auto-reconnect
 */
export default async function connect(
  urlOrConnectOptions?: string | ConnectOptions,
  socketOptions?: Record<string, unknown>,
): Promise<ElmerConnection> {
  const elmerConnection = new ElmerConnectionImpl(urlOrConnectOptions, socketOptions);
  await elmerConnection.connect();
  return elmerConnection;
}
