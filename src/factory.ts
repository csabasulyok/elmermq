import ElmerConnection, { ConnectOptions } from './api';
import ElmerConnectionImpl from './connection';

/**
 * Build AMQP connection with channel pooling and auto-reconnect
 */
export default async function connect(url: string | ConnectOptions, socketOptions?: Record<string, unknown>): Promise<ElmerConnection> {
  const elmerConnection = new ElmerConnectionImpl(url, socketOptions);
  await elmerConnection.connect();
  return elmerConnection;
}
