import { Client } from './client.js'
import type { ClientOptions, ClientConnection, SocketFactory } from './client.js'
export * from './client.js'
export {
  genericAdapter,
  wsFlowAdapter,
  RequestError,
  StreamTask
} from '@ws-flow/runtime-core'
export type {
  ProtocolAdapter,
  RequestOptions,
  RequestStreamOptions,
  StreamTaskState
} from '@ws-flow/runtime-core'
export const nativeSocketFactory: SocketFactory = (url, protocols) => {
  const socket = new WebSocket(url, protocols)
  return { socket, send: data => socket.send(data) }
}

export class WSClient<
  Events extends Record<string, any> = Record<string, any>
> extends Client<Events> {
  constructor(options: ClientOptions = {}) {
    super(options, nativeSocketFactory)
  }

  static takeover(connection: ClientConnection, options: ClientOptions = {}) {
    return new WSClient(options).attach(connection)
  }
}
