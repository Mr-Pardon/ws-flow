import { ImmediateScheduler } from './schedulers.js'
import type { WSScheduler } from './types.js'

export class SchedulerRuntime {
  private _inScheduler: WSScheduler
  private _outScheduler: WSScheduler

  constructor(inScheduler?: WSScheduler, outScheduler?: WSScheduler) {
    const defaultScheduler = new ImmediateScheduler()

    this._inScheduler = inScheduler || defaultScheduler
    this._outScheduler = outScheduler || defaultScheduler
  }

  get inScheduler() {
    return this._inScheduler
  }

  get outScheduler() {
    return this._outScheduler
  }

  setScheduler(scheduler: WSScheduler) {
    this._inScheduler = scheduler
    this._outScheduler = scheduler
  }

  setInScheduler(scheduler: WSScheduler) {
    this._inScheduler = scheduler
  }

  setOutScheduler(scheduler: WSScheduler) {
    this._outScheduler = scheduler
  }
}
