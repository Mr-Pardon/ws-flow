import type { BaseWSEnvelope } from './types.js'

export function normalizeSendInput(data: any): BaseWSEnvelope {
  const envelope: BaseWSEnvelope = isEnvelope(data) ? { ...data } : { data }

  if (envelope.streamState && !envelope.streamId) {
    throw new Error('[SendDispatcher] streamState without streamId')
  }

  return envelope
}

export function normalizeReceiveInput(data: any): BaseWSEnvelope {
  const envelope: BaseWSEnvelope = isEnvelope(data) ? { ...data } : { data }

  if (envelope.streamState && !envelope.streamId) {
    throw new Error('[ReceiveDispatcher] streamState without streamId')
  }

  return envelope
}

export function isEnvelope(data: any): data is BaseWSEnvelope {
  return !!(data && typeof data === 'object' && ('event' in data || 'streamId' in data))
}
