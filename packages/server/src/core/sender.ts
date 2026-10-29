import type {
  SendMiddleware,
  AnyRecord,
  SendOptions,
  SendStreamOptions
} from '../types/index.js'
import type { WSSRequestOptions } from '../types/request.js'
import { WSSession } from './index.js'

export type TargetResult<Session, Result> =
  | { session: Session; status: 'fulfilled'; value: Result }
  | { session: Session; status: 'rejected'; reason: unknown }

export class Sender<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> {
  private _mode: 'all' | 'filtered' = 'all'
  private _filters: ((session: WSSession<Events, Meta>) => boolean)[] = []
  private _excludes: ((session: WSSession<Events, Meta>) => boolean)[] = []
  private _localMiddlewares: SendMiddleware<Events, Meta>[] = []

  constructor(private _sessions: Set<WSSession<Events, Meta>>) {}

  private _matchSession(session: WSSession<Events, Meta>) {
    const pass = this._mode === 'all' || this._filters.every(fn => fn(session))
    const excluded = this._excludes.some(fn => fn(session))

    return pass && !excluded
  }

  private async _dispatch(fn: (session: WSSession<Events, Meta>) => Promise<any>) {
    const sessions = this._sessions

    await Promise.all(
      [...sessions].map(async session => {
        if (!this._matchSession(session)) return
        return fn(session)
      })
    )
  }

  private _mergeOptions(options?: SendOptions<Events, Meta>): SendOptions<Events, Meta> {
    if (!this._localMiddlewares.length) return options || {}

    return {
      ...options,
      sendMiddlewares: [...(options?.sendMiddlewares || []), ...this._localMiddlewares]
    }
  }

  private _mergeStreamOptions(
    options?: SendStreamOptions<Events, Meta>
  ): SendStreamOptions<Events, Meta> {
    if (!this._localMiddlewares.length) return options || {}

    return {
      ...options,
      sendMiddlewares: [...(options?.sendMiddlewares || []), ...this._localMiddlewares]
    }
  }

  use(middlewares: SendMiddleware<Events, Meta> | SendMiddleware<Events, Meta>[]) {
    const arr = Array.isArray(middlewares) ? middlewares : [middlewares]
    this._localMiddlewares.push(...arr)
    return this
  }

  to(target: string | ((session: WSSession<Events, Meta>) => boolean)) {
    this._mode = 'filtered'

    if (typeof target === 'string') {
      this._filters.push(session => session.id === target)
    } else {
      this._filters.push(target)
    }

    return this
  }

  except(target: string | ((session: WSSession<Events, Meta>) => boolean)) {
    if (typeof target === 'string') {
      this._excludes.push(session => session.id === target)
    } else {
      this._excludes.push(target)
    }

    return this
  }

  all() {
    this._mode = 'all'
    this._filters = []
    return this
  }

  async requestAll<Result = unknown>(
    event: string,
    input: unknown,
    options?: WSSRequestOptions<Events, Meta>
  ): Promise<TargetResult<WSSession<Events, Meta>, Result>[]> {
    // Capture membership and order before running any user middleware.
    const targets = [...this._sessions].filter(session => this._matchSession(session))
    const merged = { ...this._mergeOptions(options), timeoutMs: options?.timeoutMs }
    return Promise.all(
      targets.map(async session => {
        try {
          return {
            session,
            status: 'fulfilled' as const,
            value: await session.request<Result>(event, input, merged)
          }
        } catch (reason) {
          return { session, status: 'rejected' as const, reason }
        }
      })
    )
  }

  send(data: any, options?: SendOptions<Events, Meta>): Promise<void> {
    const merged = this._mergeOptions(options)
    return this._dispatch(session => session.send(data, merged))
  }

  emit(
    event: Extract<keyof Events, string>,
    data: Events[keyof Events],
    options?: SendOptions<Events, Meta>
  ): Promise<void> {
    const merged = this._mergeOptions(options)
    return this._dispatch(session => session.emit(event, data, merged))
  }

  stream(
    generator: AsyncGenerator<any> | (() => AsyncGenerator<any>),
    options?: SendStreamOptions<Events, Meta>
  ): Promise<void>

  stream(
    event: string,
    generator: AsyncGenerator<any> | (() => AsyncGenerator<any>),
    options?: SendStreamOptions<Events, Meta>
  ): Promise<void>

  async stream(arg1: any, arg2?: any, arg3?: SendStreamOptions<Events, Meta>) {
    let event: string | undefined
    let generator: AsyncGenerator<any> | (() => AsyncGenerator<any>)
    let options: SendStreamOptions<Events, Meta> | undefined

    if (typeof arg1 === 'string') {
      event = arg1
      generator = arg2
      options = arg3
    } else {
      generator = arg1
      options = arg2
    }

    const sessions = [...this._sessions].filter(session => this._matchSession(session))
    const merged = this._mergeStreamOptions(options)

    if (sessions.length > 1 && typeof generator !== 'function') {
      throw new Error(
        '[Sender] stream() requires a generator factory when sending to multiple sessions'
      )
    }

    await Promise.all(
      sessions.map(async session => {
        if (event) {
          return session.stream(event, generator, merged)
        }
        return session.stream(generator, merged)
      })
    )
  }
}
