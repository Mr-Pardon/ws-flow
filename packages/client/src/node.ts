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
export const nodeSocketFactory: SocketFactory = async (url, protocols) => {
  const { WebSocket } = await import('ws')
  const socket = new WebSocket(url, protocols)
  return {
    socket,
    send: data =>
      new Promise<void>((resolve, reject) =>
        socket.send(data, error => (error ? reject(error) : resolve()))
      )
  }
}

export class WSClient<
  Events extends Record<string, any> = Record<string, any>
> extends Client<Events> {
  constructor(options: ClientOptions = {}) {
    super(options, nodeSocketFactory)
  }

  static takeover(connection: ClientConnection, options: ClientOptions = {}) {
    return new WSClient(options).attach(connection)
  }
}
