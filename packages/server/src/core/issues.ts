import {
  WSSIssueHandler,
  WSSIssue,
  WSSIssueType,
  WSSTypedIssueHandler
} from '../types/issues.js'
import { AnyRecord } from '../types/wss.js'

export class WSSIssueHub<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> {
  private _handlers: WSSIssueHandler<Events, Meta>[] = []
  private _typedHandlers = new Map<WSSIssueType, WSSIssueHandler<Events, Meta>[]>()

  private _reportUnhandledIssue(issue: WSSIssue<Events, Meta>) {
    console.error(`[WSServer] ${issue.type}`, issue)
  }

  private _reportIssueHandlerError(error: unknown, issue: WSSIssue<Events, Meta>) {
    console.error('[WSServer] issue handler failed', error, issue)
  }

  on(handler: WSSIssueHandler<Events, Meta>): void

  on<Type extends WSSIssueType>(
    type: Type,
    handler: WSSTypedIssueHandler<Type, Events, Meta>
  ): void

  on(
    typeOrHandler: WSSIssueType | WSSIssueHandler<Events, Meta>,
    handler?: WSSIssueHandler<Events, Meta>
  ) {
    if (typeof typeOrHandler === 'function') {
      this._handlers.push(typeOrHandler)
    } else {
      const handlers = this._typedHandlers.get(typeOrHandler) || []
      handlers.push(handler!)
      this._typedHandlers.set(typeOrHandler, handlers)
    }
  }

  async emit(issue: WSSIssue<Events, Meta>) {
    const typedHandlers = this._typedHandlers.get(issue.type) || []
    const handlers = [...this._handlers, ...typedHandlers]

    if (!handlers.length) {
      this._reportUnhandledIssue(issue)
      return
    }

    const results = await Promise.allSettled(
      handlers.map(handler => Promise.resolve().then(() => handler(issue)))
    )

    results.forEach(result => {
      if (result.status === 'rejected') {
        this._reportIssueHandlerError(result.reason, issue)
      }
    })
  }

  clear() {
    this._handlers = []
    this._typedHandlers.clear()
  }
}
