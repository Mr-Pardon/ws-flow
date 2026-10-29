import { QueueBudget, ScheduledTask, throwIfAborted } from './queue.js'
import type { QueueLimits, WSTask, WSScheduler } from './types.js'

export class ComposableScheduler implements WSScheduler {
  constructor(private _schedulers: WSScheduler[]) {}

  async schedule(task: WSTask): Promise<void> {
    throwIfAborted(task)
    let i = -1

    const dispatch = async (index: number): Promise<void> => {
      throwIfAborted(task)
      if (index <= i) {
        throw new Error('next() called multiple times')
      }
      i = index

      const scheduler = this._schedulers[index]
      if (!scheduler) {
        return task.run()
      }

      await scheduler.schedule({
        ...task,
        run: async () => dispatch(index + 1)
      })
    }

    await dispatch(0)
  }
}

/** Scheduler that runs tasks immediately. */
export class ImmediateScheduler implements WSScheduler {
  async schedule(task: WSTask): Promise<void> {
    throwIfAborted(task)
    await task.run()
  }
}

/** Scheduler that runs tasks one at a time for a given key. */
export class KeyedSerialScheduler implements WSScheduler {
  private _queues = new Map<string, ScheduledTask[]>()
  private _running = new Set<string>()
  private _budget: QueueBudget

  constructor(
    private _getKey: (task: WSTask) => string | undefined,
    limits?: QueueLimits
  ) {
    this._budget = new QueueBudget(limits)
  }

  get queuedCount(): number {
    return this._budget.count
  }

  private async _run(key: string, task: ScheduledTask): Promise<void> {
    await task.run()

    const queue = this._queues.get(key)
    const next = queue?.shift()
    if (queue?.length === 0) this._queues.delete(key)

    if (next) void this._run(key, next)
    else this._running.delete(key)
  }

  async schedule(task: WSTask): Promise<void> {
    throwIfAborted(task)
    const key = this._getKey(task)

    if (!key) {
      throwIfAborted(task)
      return task.run()
    }

    return new Promise((resolve, reject) => {
      const scheduled = new ScheduledTask(task, resolve, reject)

      if (!this._running.has(key)) {
        this._running.add(key)
        void this._run(key, scheduled)
      } else {
        const release = this._budget.reserve(task.peerId)
        const queue = this._queues.get(key) ?? []
        this._queues.set(key, queue)
        queue.push(scheduled)

        scheduled.wait(release, () => {
          const index = queue.indexOf(scheduled)
          if (index !== -1) queue.splice(index, 1)
          if (queue.length === 0) this._queues.delete(key)
        })
      }
    })
  }
}

/** Scheduler that runs tasks one at a time for a given session. */
export class SessionScheduler extends KeyedSerialScheduler {
  constructor(limits?: QueueLimits) {
    super(task => task.peerId, limits)
  }
}

/** Scheduler that runs tasks in priority order, FIFO within each priority. */
export class PriorityScheduler implements WSScheduler {
  private _queue = new Map<number, ScheduledTask[]>()
  private _running = false
  private _budget: QueueBudget

  constructor(limits?: QueueLimits) {
    this._budget = new QueueBudget(limits)
  }

  get queuedCount(): number {
    return this._budget.count
  }

  private _getNextTask(): ScheduledTask | undefined {
    if (this._queue.size === 0) return

    const priorities = Array.from(this._queue.keys()).sort((a, b) => b - a)
    const priority = priorities[0]!
    const queue = this._queue.get(priority)!
    const task = queue.shift()!
    if (queue.length === 0) this._queue.delete(priority)
    return task
  }

  private async _drain(task: ScheduledTask): Promise<void> {
    this._running = true
    let next: ScheduledTask | undefined = task

    while (next) {
      await next.run()
      next = this._getNextTask()
    }
    this._running = false
  }

  async schedule(task: WSTask): Promise<void> {
    throwIfAborted(task)
    const priority = task.priority === undefined ? 0 : task.priority
    if (!Number.isFinite(priority)) {
      throw new RangeError('Task priority must be a finite number')
    }

    return new Promise((resolve, reject) => {
      const scheduled = new ScheduledTask(task, resolve, reject)

      if (!this._running) {
        void this._drain(scheduled)
      } else {
        const release = this._budget.reserve(task.peerId)
        const queue = this._queue.get(priority) ?? []
        this._queue.set(priority, queue)
        queue.push(scheduled)

        scheduled.wait(release, () => {
          const index = queue.indexOf(scheduled)
          if (index !== -1) queue.splice(index, 1)
          if (queue.length === 0) this._queue.delete(priority)
        })
      }
    })
  }
}

/** Scheduler that runs at most limit tasks concurrently. */
export class ConcurrencyScheduler implements WSScheduler {
  private _running = 0
  private _queue: ScheduledTask[] = []
  private _budget: QueueBudget

  constructor(
    private _limit: number,
    limits?: QueueLimits
  ) {
    if (!Number.isSafeInteger(_limit) || _limit < 1) {
      throw new RangeError('Concurrency limit must be a positive safe integer')
    }
    this._budget = new QueueBudget(limits)
  }

  get queuedCount(): number {
    return this._budget.count
  }

  private async _run(task: ScheduledTask): Promise<void> {
    this._running++
    await task.run()
    this._running--

    const next = this._queue.shift()
    if (next) void this._run(next)
  }

  async schedule(task: WSTask): Promise<void> {
    throwIfAborted(task)

    return new Promise((resolve, reject) => {
      const scheduled = new ScheduledTask(task, resolve, reject)

      if (this._running < this._limit) {
        void this._run(scheduled)
      } else {
        const release = this._budget.reserve(task.peerId)
        this._queue.push(scheduled)

        scheduled.wait(release, () => {
          const index = this._queue.indexOf(scheduled)
          if (index !== -1) this._queue.splice(index, 1)
        })
      }
    })
  }
}
