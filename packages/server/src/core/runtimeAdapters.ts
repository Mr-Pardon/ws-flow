import type { Middleware, EventMiddlewareRecord, PipelinePlugin } from '@ws-flow/pipeline'
import type {
  CoreReceiveContext,
  CoreSendContext,
  CoreStreamContext,
  SendOptions as CoreSendOptions,
  SendStreamOptions as CoreSendStreamOptions,
  CoreSendMiddleware,
  CoreStreamMiddleware
} from '@ws-flow/runtime-core'
import type { AnyRecord, SendOptions, SendStreamOptions } from '../types/wss.js'
import type {
  WSReceiveContext,
  WSSendContext,
  WSStreamContext
} from '../types/pipeline.js'
import type { WSSession } from './session.js'

type SessionPeer<Events extends AnyRecord, Meta extends AnyRecord> = WSSession<
  Events,
  Meta
>

type CoreReceiveCtx<
  Events extends AnyRecord,
  Meta extends AnyRecord
> = CoreReceiveContext<SessionPeer<Events, Meta>>

type CoreSendCtx<Events extends AnyRecord, Meta extends AnyRecord> = CoreSendContext<
  SessionPeer<Events, Meta>
>

type CoreStreamCtx<Events extends AnyRecord, Meta extends AnyRecord> = CoreStreamContext<
  SessionPeer<Events, Meta>
>

export function withSession<
  Events extends AnyRecord,
  Meta extends AnyRecord,
  Ctx extends { peer: SessionPeer<Events, Meta> }
>(ctx: Ctx): Ctx & { session: SessionPeer<Events, Meta> } {
  return Object.assign(ctx, { session: ctx.peer })
}

export function adaptReceiveMiddlewareToCore<
  Events extends AnyRecord,
  Meta extends AnyRecord
>(
  middleware: Middleware<WSReceiveContext<Events, Meta>>
): Middleware<CoreReceiveCtx<Events, Meta>> {
  return async (ctx, next) => middleware(withSession(ctx), next)
}

export function adaptSendMiddlewareToCore<
  Events extends AnyRecord,
  Meta extends AnyRecord
>(
  middleware: Middleware<WSSendContext<Events, Meta>>
): Middleware<CoreSendCtx<Events, Meta>> {
  return async (ctx, next) => middleware(withSession(ctx), next)
}

export function adaptStreamMiddlewareToCore<
  Events extends AnyRecord,
  Meta extends AnyRecord
>(
  middleware: Middleware<WSStreamContext<Events, Meta>>
): Middleware<CoreStreamCtx<Events, Meta>> {
  return async (ctx, next) => middleware(withSession(ctx), next)
}

export function adaptReceiveMiddlewaresToCore<
  Events extends AnyRecord,
  Meta extends AnyRecord
>(
  middlewares:
    | Middleware<WSReceiveContext<Events, Meta>>
    | Middleware<WSReceiveContext<Events, Meta>>[]
): Middleware<CoreReceiveCtx<Events, Meta>> | Middleware<CoreReceiveCtx<Events, Meta>>[] {
  return Array.isArray(middlewares)
    ? middlewares.map(middleware => adaptReceiveMiddlewareToCore(middleware))
    : adaptReceiveMiddlewareToCore(middlewares)
}

export function adaptSendMiddlewaresToCore<
  Events extends AnyRecord,
  Meta extends AnyRecord
>(
  middlewares:
    | Middleware<WSSendContext<Events, Meta>>
    | Middleware<WSSendContext<Events, Meta>>[]
): Middleware<CoreSendCtx<Events, Meta>> | Middleware<CoreSendCtx<Events, Meta>>[] {
  return Array.isArray(middlewares)
    ? middlewares.map(middleware => adaptSendMiddlewareToCore(middleware))
    : adaptSendMiddlewareToCore(middlewares)
}

export function adaptStreamMiddlewaresToCore<
  Events extends AnyRecord,
  Meta extends AnyRecord
>(
  middlewares:
    | Middleware<WSStreamContext<Events, Meta>>
    | Middleware<WSStreamContext<Events, Meta>>[]
): Middleware<CoreStreamCtx<Events, Meta>> | Middleware<CoreStreamCtx<Events, Meta>>[] {
  return Array.isArray(middlewares)
    ? middlewares.map(middleware => adaptStreamMiddlewareToCore(middleware))
    : adaptStreamMiddlewareToCore(middlewares)
}

export function adaptReceiveEventRecordsToCore<
  Events extends AnyRecord,
  Meta extends AnyRecord
>(
  records:
    | EventMiddlewareRecord<WSReceiveContext<Events, Meta>>
    | EventMiddlewareRecord<WSReceiveContext<Events, Meta>>[]
):
  | EventMiddlewareRecord<CoreReceiveCtx<Events, Meta>>
  | EventMiddlewareRecord<CoreReceiveCtx<Events, Meta>>[] {
  const adapt = (
    record: EventMiddlewareRecord<WSReceiveContext<Events, Meta>>
  ): EventMiddlewareRecord<CoreReceiveCtx<Events, Meta>> => ({
    matcher: record.matcher,
    middlewares: adaptReceiveMiddlewaresToCore(record.middlewares)
  })

  return Array.isArray(records) ? records.map(adapt) : adapt(records)
}

export function adaptSendEventRecordsToCore<
  Events extends AnyRecord,
  Meta extends AnyRecord
>(
  records:
    | EventMiddlewareRecord<WSSendContext<Events, Meta>>
    | EventMiddlewareRecord<WSSendContext<Events, Meta>>[]
):
  | EventMiddlewareRecord<CoreSendCtx<Events, Meta>>
  | EventMiddlewareRecord<CoreSendCtx<Events, Meta>>[] {
  const adapt = (
    record: EventMiddlewareRecord<WSSendContext<Events, Meta>>
  ): EventMiddlewareRecord<CoreSendCtx<Events, Meta>> => ({
    matcher: record.matcher,
    middlewares: adaptSendMiddlewaresToCore(record.middlewares)
  })

  return Array.isArray(records) ? records.map(adapt) : adapt(records)
}

export function adaptPluginToCore<Events extends AnyRecord, Meta extends AnyRecord>(
  plugin: PipelinePlugin<
    WSReceiveContext<Events, Meta>,
    WSSendContext<Events, Meta>,
    WSStreamContext<Events, Meta>
  >
): PipelinePlugin<
  CoreReceiveCtx<Events, Meta>,
  CoreSendCtx<Events, Meta>,
  CoreStreamCtx<Events, Meta>
> {
  return {
    receive: plugin.receive ? adaptReceiveMiddlewaresToCore(plugin.receive) : undefined,
    receiveEvent: plugin.receiveEvent
      ? adaptReceiveEventRecordsToCore(plugin.receiveEvent)
      : undefined,
    send: plugin.send ? adaptSendMiddlewaresToCore(plugin.send) : undefined,
    sendEvent: plugin.sendEvent
      ? adaptSendEventRecordsToCore(plugin.sendEvent)
      : undefined,
    receiveStream: plugin.receiveStream
      ? adaptStreamMiddlewaresToCore(plugin.receiveStream)
      : undefined,
    sendStream: plugin.sendStream
      ? adaptStreamMiddlewaresToCore(plugin.sendStream)
      : undefined
  }
}

export function adaptSendOptionsToCore<Events extends AnyRecord, Meta extends AnyRecord>(
  options?: SendOptions<Events, Meta>
): CoreSendOptions<SessionPeer<Events, Meta>> | undefined {
  if (!options) return options
  if (!options.sendMiddlewares?.length)
    return options as CoreSendOptions<SessionPeer<Events, Meta>>

  return {
    ...options,
    sendMiddlewares: adaptSendMiddlewaresToCore(
      options.sendMiddlewares
    ) as CoreSendMiddleware<SessionPeer<Events, Meta>>[]
  }
}

export function adaptSendStreamOptionsToCore<
  Events extends AnyRecord,
  Meta extends AnyRecord
>(
  options?: SendStreamOptions<Events, Meta>
): CoreSendStreamOptions<SessionPeer<Events, Meta>> | undefined {
  if (!options) return options

  const sendOptions = {
    ...adaptSendOptionsToCore(options),
    idleTimeoutMs: options.idleTimeoutMs
  }
  if (!options.streamMiddlewares?.length) {
    return sendOptions
  }

  return {
    ...sendOptions,
    streamMiddlewares: adaptStreamMiddlewaresToCore(
      options.streamMiddlewares
    ) as CoreStreamMiddleware<SessionPeer<Events, Meta>>[]
  }
}
