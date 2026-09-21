/** Remembers recently acknowledged event ids so Slack's retries, which carry
 * the same event id with an x-slack-retry-num header, do not start a second
 * Turn. Bounded: the oldest ids fall out once the limit is reached. */
export class SeenEvents {
  #ids = new Map();
  #limit;
  constructor(limit = 5000) {
    this.#limit = limit;
  }
  /** Marks the id as seen and reports whether it had been seen before. */
  remember(id) {
    if (this.#ids.has(id)) {
      this.#ids.delete(id);
      this.#ids.set(id, true);
      return true;
    }
    this.#ids.set(id, true);
    if (this.#ids.size > this.#limit) this.#ids.delete(this.#ids.keys().next().value);
    return false;
  }
  get size() {
    return this.#ids.size;
  }
}
