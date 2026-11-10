type Listener<P> = (payload: P) => void;

/** Surface a listener error without letting it unwind the emitter: `reportError` where the platform has it, else as an uncaught exception. */
const report = (e: unknown): void =>
  typeof reportError === 'function'
    ? reportError(e)
    : queueMicrotask(() => {
        throw e;
      });

/**
 * Minimal typed event emitter. Listeners are copied before dispatch so handlers may unsubscribe safely.
 * A throwing listener is reported (see `report`) and never prevents the remaining listeners or the emitter's caller from running.
 */
export class Emitter<E extends Record<string, unknown>> {
  private listeners: { [K in keyof E]?: Set<Listener<E[K]>> } = {};

  /** Subscribe; returns the unsubscribe function. */
  on<K extends keyof E>(event: K, fn: Listener<E[K]>): () => void {
    (this.listeners[event] ??= new Set()).add(fn);
    return () => void this.listeners[event]?.delete(fn);
  }

  protected has(event: keyof E): boolean {
    return !!this.listeners[event]?.size;
  }

  protected emit<K extends keyof E>(event: K, payload: E[K]): void {
    const set = this.listeners[event];
    if (!set) return;
    for (const fn of [...set]) {
      try {
        fn(payload);
      } catch (e) {
        report(e);
      }
    }
  }
}
