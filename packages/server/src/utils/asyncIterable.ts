export const isAsyncGenerator = (obj: any): obj is AsyncGenerator<any> => {
  return !!(
    obj &&
    typeof obj === 'object' &&
    typeof obj[Symbol.asyncIterator] === 'function'
  )
}

export const resolveAsyncGenerator = (input: any): AsyncGenerator<any> | null => {
  if (isAsyncGenerator(input)) return input

  if (typeof input === 'function') {
    const res = input()
    if (isAsyncGenerator(res)) return res
  }

  return null
}
