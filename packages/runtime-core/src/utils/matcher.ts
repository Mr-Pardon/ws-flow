import { EventMatcher } from '@ws-flow/pipeline'

export const matchEvent = (event: string | undefined, matcher: EventMatcher) => {
  if (!event) return false

  if (typeof matcher === 'string') {
    return matcher === event
  }

  if (matcher instanceof RegExp) {
    return matcher.test(event)
  }

  if (typeof matcher === 'function') {
    return matcher(event)
  }

  return false
}
