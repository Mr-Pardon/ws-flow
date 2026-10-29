import { 
  BaseProtocol, 
  type Protocol as ProxyProtocol,
  type RouteRule
} from 'ws-event-proxy'
import type {
  AnyRecord
} from '../types/index.js'

export interface WSProtocol<
  Events extends AnyRecord = AnyRecord
> extends Partial<ProxyProtocol> {
  reconnectTimeout: number;
  resolveEventType<Type extends keyof Events>(
    name: Type
  ): RouteRule<Events[Type]>

  /**
   * @deprecated Use top-level proxy fields instead.
   */
  proxy?: Partial<ProxyProtocol>
}

export const WSProtocol: WSProtocol<any> = {
  reconnectTimeout: 2000,
  resolveEventType(name) {
    return { type: name }
  },
  ...BaseProtocol,
}
