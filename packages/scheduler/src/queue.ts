import type { QueueLimits, WSTask } from './types.js'

export class QueueCapacityError extends Error {
  readonly code = 'QUEUE_CAPACITY_EXCEEDED'

  constructor(
    readonly scope: 'global' | 'peer',
    readonly limit: number,
    readonly peerId?: string
  ) {
    super(`Scheduler ${scope} waiting queue capacity exceeded (${limit})`)
    this.name = 'QueueCapacityError'
  }
}

export function throwIfAborted(task: WSTask): void {
  if (task.signal?.aborted) throw task.signal.reason
}

type AbortGroup = {
  listeners: Set<() => void>
  onAbort: () => void
}

const abortGroups = new WeakMap<AbortSignal, AbortGroup>()

/** A shared connection signal needs only one native listener for all queued tasks. */
function subscribeAbort(signal: AbortSignal, listener: () => void): () => void {
  let group = abortGroups.get(signal)
  if (!group) {
    const listeners = new Set<() => void>()
    group = {
      listeners,
      onAbort: () => {
        abortGroups.delete(signal)
        const pending = [...listeners]
        listeners.clear()
        for (const notify of pending) notify()
      }
    }
    abortGroups.set(signal, group)
    signal.addEventListener('abort', group.onAbort, { once: true })
  }
  group.listeners.add(listener)

  return () => {
    group.listeners.delete(listener)
    if (group.listeners.size === 0 && abortGroups.get(signal) === group) {
      signal.removeEventListener('abort', group.onAbort)
      abortGroups.delete(signal)
    }
  }
}

/** Reservations belong only to queued entries, never to running tasks. */
export class QueueBudget {
  private _count = 0
  private _peers = new Map<string | undefined, number>()
  private readonly _limits: QueueLimits

  constructor(limits: QueueLimits = {}) {
    if (!limits || typeof limits !== 'object' || Array.isArray(limits)) {
      throw new TypeError('Queue limits must be an object')
    }
    for (const name of ['maxQueued', 'maxQueuedPerPeer'] as const) {
      const limit = limits[name]
      if (limit !== undefined && (!Number.isSafeInteger(limit) || limit < 1)) {
        throw new RangeError(`${name} must be a positive safe integer`)
      }
    }
    this._limits = { ...limits }
  }

  get count(): number {
    return this._count
  }

  reserve(peerId: string | undefined): () => void {
    const { maxQueued, maxQueuedPerPeer } = this._limits
    const peerCount = this._peers.get(peerId) ?? 0

    if (maxQueued !== undefined && this._count >= maxQueued) {
      throw new QueueCapacityError('global', maxQueued, peerId)
    }
    if (maxQueuedPerPeer !== undefined && peerCount >= maxQueuedPerPeer) {
      throw new QueueCapacityError('peer', maxQueuedPerPeer, peerId)
    }

    this._count++
    this._peers.set(peerId, peerCount + 1)
    let released = false

    return () => {
      if (released) return
      released = true
      this._count--

      const remaining = this._peers.get(peerId)! - 1
      if (remaining === 0) this._peers.delete(peerId)
      else this._peers.set(peerId, remaining)
    }
  }
}

/** Owns a queued task's abort listener and settles its caller exactly once. */
export class ScheduledTask {
  private _release?: () => void
  private _remove?: () => void
  private _unsubscribe?: () => void
  private _abort = () => {
    const task = this._task
    if (!task) return

    this._task = undefined
    const remove = this._remove
    this._leaveQueue()
    remove?.()
    this._settle(false, task.signal?.reason)
  }

  constructor(
    private _task: WSTask | undefined,
    private _resolve: (() => void) | undefined,
    private _reject: ((reason: unknown) => void) | undefined
  ) {}

  wait(release: () => void, remove: () => void): void {
    this._release = release
    this._remove = remove
    const signal = this._task?.signal
    if (signal) this._unsubscribe = subscribeAbort(signal, this._abort)
    if (signal?.aborted) this._abort()
  }

  async run(): Promise<void> {
    const task = this._task
    if (!task) return

    this._task = undefined
    this._leaveQueue()

    try {
      throwIfAborted(task)
      await task.run()
      this._settle(true)
    } catch (error) {
      this._settle(false, error)
    }
  }

  private _leaveQueue(): void {
    this._unsubscribe?.()
    this._unsubscribe = undefined
    this._release?.()
    this._release = undefined
    this._remove = undefined
  }

  private _settle(success: boolean, reason?: unknown): void {
    const resolve = this._resolve
    const reject = this._reject
    this._resolve = undefined
    this._reject = undefined
    if (success) resolve?.()
    else reject?.(reason)
  }
}
