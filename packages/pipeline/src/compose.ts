import type { Middleware } from './types.js'

export const composeMiddleware = <Ctx>(middlewares: Middleware<Ctx>[]) => {
  return (ctx: Ctx) => {
    let index = -1

    const dispatch = (i: number): Promise<void> => {
      if (i <= index) return Promise.reject(new Error('next() called multiple times'))

      index = i
      const fn = middlewares[i]

      if (!fn) return Promise.resolve()

      try {
        return Promise.resolve(fn(ctx, () => dispatch(i + 1)))
      } catch (err) {
        return Promise.reject(err)
      }
    }

    return dispatch(0)
  }
}
