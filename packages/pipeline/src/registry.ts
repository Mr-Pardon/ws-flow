import type { PipelinePlugin, Middleware, EventMiddlewareRecord } from './types.js'

export class PipelineRegistry<ReceiveCtx, SendCtx, StreamCtx> {
  private _receiveMiddlewares: Middleware<ReceiveCtx>[] = []
  private _receiveEventMiddlewares: EventMiddlewareRecord<ReceiveCtx>[] = []
  private _receiveStreamMiddlewares: Middleware<StreamCtx>[] = []

  private _sendMiddlewares: Middleware<SendCtx>[] = []
  private _sendEventMiddlewares: EventMiddlewareRecord<SendCtx>[] = []
  private _sendStreamMiddlewares: Middleware<StreamCtx>[] = []

  private _toArray<T>(input: T | T[]) {
    return Array.isArray(input) ? input : [input]
  }

  private _applyPlugin(plugin: PipelinePlugin<ReceiveCtx, SendCtx, StreamCtx>) {
    if (plugin.receive) this.useReceive(plugin.receive)
    if (plugin.receiveEvent) this.useReceiveEvent(plugin.receiveEvent)
    if (plugin.receiveStream) this.useReceiveStream(plugin.receiveStream)
    if (plugin.send) this.useSend(plugin.send)
    if (plugin.sendEvent) this.useSendEvent(plugin.sendEvent)
    if (plugin.sendStream) this.useSendStream(plugin.sendStream)
  }

  use(
    plugins:
      | PipelinePlugin<ReceiveCtx, SendCtx, StreamCtx>
      | PipelinePlugin<ReceiveCtx, SendCtx, StreamCtx>[]
  ) {
    const list = this._toArray(plugins)
    list.forEach(plugin => this._applyPlugin(plugin))

    return this
  }

  useReceive(middlewares: Middleware<ReceiveCtx> | Middleware<ReceiveCtx>[]) {
    const arr = this._toArray(middlewares)
    this._receiveMiddlewares.push(...arr)

    return this
  }

  useReceiveEvent(
    records: EventMiddlewareRecord<ReceiveCtx> | EventMiddlewareRecord<ReceiveCtx>[]
  ) {
    const arr = this._toArray(records)
    this._receiveEventMiddlewares.push(...arr)

    return this
  }

  useReceiveStream(middlewares: Middleware<StreamCtx> | Middleware<StreamCtx>[]) {
    const arr = this._toArray(middlewares)
    this._receiveStreamMiddlewares.push(...arr)

    return this
  }

  useSend(middlewares: Middleware<SendCtx> | Middleware<SendCtx>[]) {
    const arr = this._toArray(middlewares)
    this._sendMiddlewares.push(...arr)

    return this
  }

  useSendEvent(
    records: EventMiddlewareRecord<SendCtx> | EventMiddlewareRecord<SendCtx>[]
  ) {
    const arr = this._toArray(records)
    this._sendEventMiddlewares.push(...arr)

    return this
  }

  useSendStream(middlewares: Middleware<StreamCtx> | Middleware<StreamCtx>[]) {
    const arr = this._toArray(middlewares)
    this._sendStreamMiddlewares.push(...arr)

    return this
  }

  get receiveMiddlewares() {
    return this._receiveMiddlewares
  }

  get receiveEventMiddlewares() {
    return this._receiveEventMiddlewares
  }

  get receiveStreamMiddlewares() {
    return this._receiveStreamMiddlewares
  }

  get sendMiddlewares() {
    return this._sendMiddlewares
  }

  get sendEventMiddlewares() {
    return this._sendEventMiddlewares
  }

  get sendStreamMiddlewares() {
    return this._sendStreamMiddlewares
  }
}
