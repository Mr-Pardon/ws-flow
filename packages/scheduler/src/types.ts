export interface WSTask {
  run: () => Promise<void>

  type?: 'send' | 'receive'
  peerId?: string
  room?: string

  priority?: number

  /** Cancels admission or removes waiting work; running work must cooperate. */
  signal?: AbortSignal

  meta?: Record<string, any>
}

export interface WSScheduler {
  schedule(task: WSTask): Promise<void>
}

/** Limits waiting tasks only. Omitted capacities are unlimited. */
export interface QueueLimits {
  maxQueued?: number
  maxQueuedPerPeer?: number
}

export type SchedulerType = 'immediate' | 'concurrency'
