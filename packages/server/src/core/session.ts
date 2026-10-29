import { WebSocket } from 'ws'
import type {
  AnyRecord,
  SendOptions,
  SendStreamOptions,
  WSEnvelope,
  WSSessionDeps
} from '../types/index.js'
import { resolveAsyncGenerator } from '../utils/index.js'
import type {
  WSSRequestOptions,
  WSSRequestHandler,
  WSSRequestStreamOptions,
  WSSStreamHandler
} from '../types/request.js'
import type {
  SubscriptionFilter,
  SubscriptionOptions,
  ReceivedMessage
} from '@ws-flow/runtime-core'

let SESSION_ID = 0

export type WSSessionState = 'open' | 'closing' | 'closed'

export class WSSession<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> {
  private _id: string
  private _ws: WebSocket
  private _deps: WSSessionDeps<Events, Meta>
  private _state: WSSessionState = 'open'

  public meta: Partial<Meta> = {}

  constructor(ws: WebSocket, deps: WSSessionDeps<Events, Meta>) {
    this._deps = deps
    this._id = this._genId()
    this._ws = ws
  }

  private _genId() {
    return `sess_${SESSION_ID++}`
  }

  _markClosed() {
    this._state = 'closed'
  }

  join(room: string) {
    if (this._state !== 'open') throw new Error('Session is closed')
    this._deps.roomManager.join(this, room)
  }

  leave(room: string) {
    this._deps.roomManager.leave(this, room)
  }

  leaveAll() {
    this._deps.roomManager.leaveAll(this)
  }

  send(data: any, options?: SendOptions<Events, Meta>) {
    const envelope = this._deps.envelopeBuilder.createMessage({
      data,
      meta: options?.meta,
      priority: options?.priority
    })

    return this._deps.sendDispatcher.dispatch(this, envelope, options)
  }

  request<Result = unknown>(
    event: string,
    input: unknown,
    options?: WSSRequestOptions<Events, Meta>
  ): Promise<Result> {
    return this._deps.request<Result>(this, event, input, options)
  }

  handleRequest<Input = unknown, Result = unknown>(
    event: string,
    handler: WSSRequestHandler<Events, Meta, Input, Result>
  ) {
    return this._deps.handleRequest(this, event, handler)
  }

  requestStream<Chunk = unknown>(
    event: string,
    input: unknown,
    options?: WSSRequestStreamOptions<Events, Meta>
  ) {
    return this._deps.requestStream<Chunk>(this, event, input, options)
  }

  handleStream<Input = unknown, Chunk = unknown>(
    event: string,
    handler: WSSStreamHandler<Events, Meta, Input, Chunk>
  ) {
    return this._deps.handleStream(this, event, handler)
  }

  subscribe(
    filter: SubscriptionFilter<WSSession<Events, Meta>>,
    options?: Omit<SubscriptionOptions<WSSession<Events, Meta>>, 'peer'>
  ) {
    return this._deps.subscribe(this, filter, options)
  }

  on(
    event: string,
    handler: (
      data: unknown,
      message: ReceivedMessage<WSSession<Events, Meta>>
    ) => void | Promise<void>
  ) {
    const subscription = this.subscribe((_, message) => message.event === event)
    void (async () => {
      try {
        for await (const message of subscription) await handler(message.data, message)
      } catch (error) {
        this._deps.report(error, this)
      } finally {
        subscription.cancel()
      }
    })()
    return () => subscription.cancel()
  }

  emit(
    event: Extract<keyof Events, string>,
    data: Events[keyof Events],
    options?: SendOptions<Events, Meta>
  ) {
    const envelope = this._deps.envelopeBuilder.createMessage({
      event,
      data,
      meta: options?.meta,
      priority: options?.priority
    })

    return this._deps.sendDispatcher.dispatch(this, envelope, options)
  }

  sendEnvelope(envelope: WSEnvelope<Events>, options?: SendOptions<Events, Meta>) {
    return this._deps.sendDispatcher.dispatch(this, envelope, options)
  }

  stream(
    event: string,
    generator: AsyncGenerator<any> | (() => AsyncGenerator<any>),
    options?: SendStreamOptions<Events, Meta>
  ): Promise<void>

  stream(
    generator: AsyncGenerator<any> | (() => AsyncGenerator<any>),
    options?: SendStreamOptions<Events, Meta>
  ): Promise<void>

  stream(
    arg1: string | AsyncGenerator<any> | (() => AsyncGenerator<any>),
    arg2?:
      | AsyncGenerator<any>
      | (() => AsyncGenerator<any>)
      | SendStreamOptions<Events, Meta>,
    arg3?: SendStreamOptions<Events, Meta>
  ) {
    let event: string | undefined
    let generatorInput: AsyncGenerator<any> | (() => AsyncGenerator<any>)
    let options: SendStreamOptions<Events, Meta> | undefined

    if (typeof arg1 === 'string') {
      event = arg1
      generatorInput = arg2 as AsyncGenerator<any> | (() => AsyncGenerator<any>)
      options = arg3
    } else {
      generatorInput = arg1
      options = arg2 as SendStreamOptions<Events, Meta>
    }

    const generator = resolveAsyncGenerator(generatorInput)
    if (!generator) {
      throw new Error(
        '[WSSession] stream() requires an AsyncGenerator or generator factory'
      )
    }

    return this._deps.createSendStream(this, event, options).runGenerator(generator)
  }

  close() {
    if (this._state !== 'open') return
    this._state = 'closing'
    this._ws.close()
  }

  get id() {
    return this._id
  }

  get ws() {
    return this._ws
  }

  get state() {
    return this._state
  }

  get rooms(): ReadonlySet<string> {
    return this._deps.roomManager.getRoomsOf(this)
  }
}
