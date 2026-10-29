import type { AnyRecord } from '../types/index.js'
import type { WSSession } from './session.js'

export class RoomManager<
  Events extends AnyRecord = AnyRecord,
  Meta extends AnyRecord = AnyRecord
> {
  private _rooms = new Map<string, Set<WSSession<Events, Meta>>>()

  join(session: WSSession<Events, Meta>, room: string): void {
    let set = this._rooms.get(room)
    if (!set) {
      set = new Set()
      this._rooms.set(room, set)
    }
    set.add(session)
  }

  leave(session: WSSession<Events, Meta>, room: string): void {
    const set = this._rooms.get(room)
    if (!set) return
    set.delete(session)
    if (set.size === 0) {
      this._rooms.delete(room)
    }
  }

  leaveAll(session: WSSession<Events, Meta>): void {
    for (const [room, set] of this._rooms) {
      if (set.has(session)) {
        set.delete(session)
        if (set.size === 0) {
          this._rooms.delete(room)
        }
      }
    }
  }

  /** 查询 session 所属的所有房间 —— O(总房间数) */
  getRoomsOf(session: WSSession<Events, Meta>): Set<string> {
    const result = new Set<string>()
    for (const [room, set] of this._rooms) {
      if (set.has(session)) result.add(room)
    }
    return result
  }

  getSessions(room: string): Set<WSSession<Events, Meta>> | undefined {
    return this._rooms.get(room)
  }

  get rooms(): ReadonlyMap<string, Set<WSSession<Events, Meta>>> {
    return this._rooms
  }
}
