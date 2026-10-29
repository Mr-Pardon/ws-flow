import { abortable } from './abortable.js'

/** Owns dispatcher waits; cancellation does not pretend user code has stopped. */
export class PeerTasks<Peer extends object> {
  private _tasks = new Map<Peer, Set<AbortController>>()
  private _parents = new Map<
    AbortSignal,
    {
      controllers: Set<AbortController>
      abort: () => void
    }
  >()

  private _link(parent: AbortSignal | undefined, controller: AbortController) {
    if (!parent) return () => {}
    let group = this._parents.get(parent)
    if (!group) {
      const controllers = new Set<AbortController>()
      const abort = () => {
        for (const child of [...controllers]) child.abort(parent.reason)
      }
      group = { controllers, abort }
      this._parents.set(parent, group)
      parent.addEventListener('abort', abort, { once: true })
    }
    group.controllers.add(controller)
    return () => {
      group.controllers.delete(controller)
      if (!group.controllers.size) {
        parent.removeEventListener('abort', group.abort)
        this._parents.delete(parent)
      }
    }
  }

  async run<T>(
    peer: Peer,
    parent: AbortSignal | undefined,
    operation: (signal: AbortSignal) => Promise<T>
  ): Promise<T> {
    parent?.throwIfAborted()
    const controller = new AbortController()
    const unlink = this._link(parent, controller)
    let tasks = this._tasks.get(peer)
    if (!tasks) this._tasks.set(peer, (tasks = new Set()))
    tasks.add(controller)

    try {
      return await abortable(operation(controller.signal), controller.signal)
    } finally {
      unlink()
      tasks.delete(controller)
      if (!tasks.size && this._tasks.get(peer) === tasks) this._tasks.delete(peer)
    }
  }

  close(peer: Peer, reason: unknown) {
    const tasks = this._tasks.get(peer)
    this._tasks.delete(peer)
    for (const controller of tasks ?? []) controller.abort(reason)
  }

  dispose(reason: unknown = new Error('Runtime is closed')) {
    for (const peer of [...this._tasks.keys()]) this.close(peer, reason)
  }
}
