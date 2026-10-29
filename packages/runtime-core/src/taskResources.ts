export type ActiveTaskKind =
  | 'incomingRequests'
  | 'outgoingRequests'
  | 'incomingStreams'
  | 'outgoingStreams'

export interface ActiveTaskLimits {
  maxActive?: number
  maxActivePerPeer?: number
}

export interface RuntimeTaskLimits extends ActiveTaskLimits {
  incomingRequests?: ActiveTaskLimits
  outgoingRequests?: ActiveTaskLimits
  incomingStreams?: ActiveTaskLimits
  outgoingStreams?: ActiveTaskLimits
}

export interface RuntimeTaskTimeouts {
  incomingRequestMs?: number | null
  streamIdleMs?: number | null
}

export class TaskCapacityError extends Error {
  readonly code = 'CAPACITY_EXCEEDED'

  constructor(
    readonly kind: ActiveTaskKind,
    readonly scope: 'global' | 'peer',
    readonly limit: number,
    readonly actual: number,
    readonly budget: 'all' | ActiveTaskKind
  ) {
    super(`Active task capacity exceeded: ${budget} ${scope} limit is ${limit}`)
    this.name = 'TaskCapacityError'
  }
}

export class TaskTimeoutError extends Error {
  constructor(
    readonly code: 'EXECUTION_TIMEOUT' | 'STREAM_IDLE_TIMEOUT',
    readonly timeoutMs: number
  ) {
    super(
      code === 'EXECUTION_TIMEOUT'
        ? `Incoming request execution exceeded ${timeoutMs} ms`
        : `Stream was idle for ${timeoutMs} ms`
    )
    this.name = 'TaskTimeoutError'
  }
}

const TASK_KINDS: readonly ActiveTaskKind[] = [
  'incomingRequests',
  'outgoingRequests',
  'incomingStreams',
  'outgoingStreams'
]

const MAX_TIMER_MS = 2 ** 31 - 1

function positiveInteger(value: unknown, name: string, max = Number.MAX_SAFE_INTEGER) {
  if (!Number.isSafeInteger(value) || (value as number) < 1 || (value as number) > max)
    throw new RangeError(`${name} must be a positive safe integer no greater than ${max}`)
}

function validateObject(value: unknown, name: string) {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new TypeError(`${name} must be an object`)
}

function validateLimits(limits: ActiveTaskLimits, name: string): ActiveTaskLimits {
  validateObject(limits, name)
  const copy: ActiveTaskLimits = {}
  for (const field of ['maxActive', 'maxActivePerPeer'] as const) {
    if (limits[field] !== undefined) {
      positiveInteger(limits[field], `${name}.${field}`)
      copy[field] = limits[field]
    }
  }
  return copy
}

function validateKind(kind: ActiveTaskKind) {
  if (!TASK_KINDS.includes(kind)) throw new TypeError(`Unknown active task kind: ${kind}`)
}

function emptyCounts(): Record<ActiveTaskKind, number> {
  return {
    incomingRequests: 0,
    outgoingRequests: 0,
    incomingStreams: 0,
    outgoingStreams: 0
  }
}

type PeerCounts = { total: number; kinds: Record<ActiveTaskKind, number> }

/** Counts logical registered tasks, including those waiting for admission to execution. */
export class ActiveTaskBudget<Peer extends object = object> {
  private readonly _limits: ActiveTaskLimits
  private readonly _kindLimits: Record<ActiveTaskKind, ActiveTaskLimits>
  private readonly _peers = new Map<Peer, PeerCounts>()
  private _kinds = emptyCounts()
  private _total = 0

  constructor(limits: RuntimeTaskLimits = {}) {
    this._limits = validateLimits(limits, 'taskLimits')
    this._kindLimits = {
      incomingRequests: { maxActivePerPeer: 1024 },
      outgoingRequests: { maxActivePerPeer: 1024 },
      incomingStreams: {},
      outgoingStreams: {}
    }
    for (const kind of TASK_KINDS) {
      if (limits[kind] !== undefined) {
        Object.assign(
          this._kindLimits[kind],
          validateLimits(limits[kind], `taskLimits.${kind}`)
        )
      }
    }
  }

  get activeCount() {
    return this._total
  }

  getActiveCount(peer?: Peer, kind?: ActiveTaskKind) {
    if (kind !== undefined) validateKind(kind)
    if (peer === undefined) return kind === undefined ? this._total : this._kinds[kind]
    const counts = this._peers.get(peer)
    return kind === undefined ? (counts?.total ?? 0) : (counts?.kinds[kind] ?? 0)
  }

  reserve(peer: Peer, kind: ActiveTaskKind): () => void {
    validateKind(kind)
    if ((!peer || typeof peer !== 'object') && typeof peer !== 'function')
      throw new TypeError('Task peer must be an object')
    const counts = this._peers.get(peer)
    const kindLimits = this._kindLimits[kind]
    this._check(kind, 'global', this._limits.maxActive, this._total, 'all')
    this._check(kind, 'peer', this._limits.maxActivePerPeer, counts?.total ?? 0, 'all')
    this._check(kind, 'global', kindLimits.maxActive, this._kinds[kind], kind)
    this._check(kind, 'peer', kindLimits.maxActivePerPeer, counts?.kinds[kind] ?? 0, kind)

    const reservation = counts ?? { total: 0, kinds: emptyCounts() }
    this._peers.set(peer, reservation)
    reservation.total++
    reservation.kinds[kind]++
    this._total++
    this._kinds[kind]++
    let active = true
    return () => {
      if (!active) return
      active = false
      if (this._peers.get(peer) !== reservation) return
      reservation.total--
      reservation.kinds[kind]--
      this._total--
      this._kinds[kind]--
      if (reservation.total === 0) this._peers.delete(peer)
    }
  }

  closePeer(peer: Peer) {
    const counts = this._peers.get(peer)
    if (!counts) return
    this._peers.delete(peer)
    this._total -= counts.total
    for (const kind of TASK_KINDS) this._kinds[kind] -= counts.kinds[kind]
  }

  clear() {
    this._peers.clear()
    this._kinds = emptyCounts()
    this._total = 0
  }

  private _check(
    kind: ActiveTaskKind,
    scope: 'global' | 'peer',
    limit: number | undefined,
    count: number,
    budget: 'all' | ActiveTaskKind
  ) {
    if (limit !== undefined && count >= limit)
      throw new TaskCapacityError(kind, scope, limit, count + 1, budget)
  }
}

function validateTimeout(value: unknown, name: string): number | null {
  if (value === undefined || value === null) return null
  positiveInteger(value, name, MAX_TIMER_MS)
  return value as number
}

/** Timeouts are opt-in. Undefined and null both disable the associated deadline. */
export function validateTaskTimeouts(
  timeouts: RuntimeTaskTimeouts = {}
): Required<RuntimeTaskTimeouts> {
  validateObject(timeouts, 'taskTimeouts')
  return {
    incomingRequestMs: validateTimeout(timeouts.incomingRequestMs, 'incomingRequestMs'),
    streamIdleMs: validateTimeout(timeouts.streamIdleMs, 'streamIdleMs')
  }
}

/** A monotonic idle deadline that starts on first touch and never revives after expiry. */
export class IdleDeadline {
  private readonly _timeoutMs: number | null
  private _timer?: ReturnType<typeof setTimeout>
  private _deadline?: number
  private _generation = 0
  private _closed = false

  constructor(
    timeoutMs: number | null | undefined,
    private readonly _onTimeout: () => void
  ) {
    this._timeoutMs = validateTimeout(timeoutMs, 'idleTimeoutMs')
  }

  touch() {
    if (this._closed || this._timeoutMs === null) return
    const now = performance.now()
    if (this._deadline !== undefined && now >= this._deadline) {
      this._expire()
      return
    }
    this._deadline = now + this._timeoutMs
    this._arm(this._timeoutMs)
  }

  check() {
    if (this._closed || this._timeoutMs === null || this._deadline === undefined) return
    if (performance.now() >= this._deadline) this._expire()
  }

  clear() {
    if (this._closed) return
    this._closed = true
    this._generation++
    clearTimeout(this._timer)
    this._timer = undefined
  }

  private _arm(waitMs: number) {
    clearTimeout(this._timer)
    const generation = ++this._generation
    this._timer = setTimeout(() => {
      if (this._closed || generation !== this._generation) return
      const remaining = this._deadline! - performance.now()
      if (remaining > 0) this._arm(Math.ceil(remaining))
      else this._expire()
    }, waitMs)
  }

  private _expire() {
    if (this._closed) return
    this.clear()
    this._onTimeout()
  }
}
