import type { StreamTask } from './stream-task.js'
import type { AnyRecord } from '../types/ws.js'
import type {
  Middleware,
  StreamMiddleware,
  StreamCtx,
  WSMessageContext,
  WSSendContext
} from '../types/middleware.js'

export const composeMiddleware = <T, Events extends AnyRecord>(
  middlewares: Middleware<T, Events>[],
) => {
  return async (
    ctx:
      | WSMessageContext<T, Events>
      | WSSendContext<T, Events>
  ) => {
    let index = -1

    const dispatch = async (i: number) => {
      if (i <= index) return Promise.reject(new Error('next() called multiple times'))
      index = i
      
      const fn = middlewares[i]
      if (!fn) return
      
      await fn(ctx, () => dispatch(i + 1))
    }

    await dispatch(0)
  }
}

export const composeStreamMiddleware = <Receive, Events extends AnyRecord>(
  middlewares: StreamMiddleware<Receive, Events>[],
) => {
  return (
    task: StreamTask<Receive, Events>,
    ctx: StreamCtx<Receive, Events>
  ): StreamTask<Receive, Events> => {
    let index = -1

    const dispatch = (i: number, currentTask: StreamTask<Receive, Events>): StreamTask<Receive, Events> => {
      if (i <= index) {
        throw new Error('next() called multiple times')
      }
      index = i
      
      const fn = middlewares[i]
      if (!fn) return currentTask
      
      return fn(currentTask, ctx, (nextTask?: StreamTask<Receive, Events>) => 
        dispatch(i + 1, nextTask ?? currentTask))
    }

    return dispatch(0, task)
  }
}
