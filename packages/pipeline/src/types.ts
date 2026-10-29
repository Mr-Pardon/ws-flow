export type Middleware<Ctx> = (
  ctx: Ctx,
  next: () => Promise<void>
) => Promise<void> | void

export type EventMatcher = string | RegExp | ((event: string) => boolean)

export type EventMiddlewareRecord<Ctx> = {
  matcher: EventMatcher
  middlewares: Middleware<Ctx> | Middleware<Ctx>[]
}

export type PipelinePlugin<ReceiveCtx, SendCtx, StreamCtx> = {
  receive?: Middleware<ReceiveCtx> | Middleware<ReceiveCtx>[]
  receiveEvent?: EventMiddlewareRecord<ReceiveCtx> | EventMiddlewareRecord<ReceiveCtx>[]
  send?: Middleware<SendCtx> | Middleware<SendCtx>[]
  sendEvent?: EventMiddlewareRecord<SendCtx> | EventMiddlewareRecord<SendCtx>[]
  receiveStream?: Middleware<StreamCtx> | Middleware<StreamCtx>[]
  sendStream?: Middleware<StreamCtx> | Middleware<StreamCtx>[]
}
