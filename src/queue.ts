/**
 * In-memory FIFO queue to store some messages
 * which are waiting for an active connection
 */
export default class FifoQueue<T> {
  private messages: Record<number, T>;
  private readIdx: number;
  private writeIdx: number;

  numUnsentMessages: number;

  constructor() {
    this.messages = {};
    this.readIdx = 0;
    this.writeIdx = 0;

    this.numUnsentMessages = 0;
  }

  /**
   * Check if there is anything backed up
   */
  get backedUp(): boolean {
    return this.numUnsentMessages > 0;
  }

  /**
   * Push message to write end of queue
   */
  push(message: T): number {
    const idx = this.writeIdx;
    this.messages[idx] = message;

    this.numUnsentMessages += 1;
    this.writeIdx += 1;
    return idx;
  }

  /**
   * Pop message from read end of queue.
   */
  consume(): T {
    const idx = this.readIdx;
    const ret = this.messages[idx];

    this.numUnsentMessages -= 1;
    this.readIdx += 1;
    return ret;
  }
}
