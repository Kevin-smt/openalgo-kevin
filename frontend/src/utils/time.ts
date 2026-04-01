export const IST_TIME_ZONE = 'Asia/Kolkata'
const IST_OFFSET_MINUTES = 330
const IST_OFFSET_SUFFIX = '+05:30'

function pad(value: number): string {
  return String(value).padStart(2, '0')
}

function partsForIst(date: Date) {
  return new Intl.DateTimeFormat('en-GB', {
    timeZone: IST_TIME_ZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).formatToParts(date)
}

function buildPartsMap(date: Date) {
  return partsForIst(date).reduce<Record<string, string>>((acc, part) => {
    if (part.type !== 'literal') acc[part.type] = part.value
    return acc
  }, {})
}

function hasExplicitTimezone(timestamp: string): boolean {
  return /(?:Z|[+-]\d{2}:?\d{2})$/i.test(timestamp)
}

function parseNumericTimestamp(timestamp: number): Date {
  const normalized = Math.abs(timestamp) < 1_000_000_000_000 ? timestamp * 1000 : timestamp
  return new Date(normalized)
}

function parseIstTimestamp(timestamp: string | number | Date): Date {
  if (timestamp instanceof Date) return timestamp
  if (typeof timestamp === 'number') return parseNumericTimestamp(timestamp)

  const normalized = timestamp.trim()
  if (!normalized) return new Date(NaN)

  if (/^\d+$/.test(normalized)) {
    return parseNumericTimestamp(Number(normalized))
  }

  if (hasExplicitTimezone(normalized)) {
    return new Date(normalized)
  }

  const isoLike = normalized.includes('T') ? normalized : normalized.replace(' ', 'T')
  const asIst = new Date(`${isoLike}${IST_OFFSET_SUFFIX}`)
  if (!Number.isNaN(asIst.getTime())) return asIst

  const match = normalized.match(
    /^(\d{2})[-/](\d{2})[-/](\d{4})(?:[ T](\d{2}):(\d{2})(?::(\d{2}))?)?$/
  )
  if (match) {
    const [, day, month, year, hour = '00', minute = '00', second = '00'] = match
    return new Date(`${year}-${month}-${day}T${hour}:${minute}:${second}${IST_OFFSET_SUFFIX}`)
  }

  return new Date(normalized)
}

function parseUtcTimestamp(timestamp: string | number | Date): Date {
  if (timestamp instanceof Date) return timestamp
  if (typeof timestamp === 'number') return parseNumericTimestamp(timestamp)

  const normalized = timestamp.trim()
  if (!normalized) return new Date(NaN)

  if (/^\d+$/.test(normalized)) {
    return parseNumericTimestamp(Number(normalized))
  }

  if (hasExplicitTimezone(normalized)) {
    return new Date(normalized)
  }

  const isoLike = normalized.includes('T') ? normalized : normalized.replace(' ', 'T')
  const asUtc = new Date(`${isoLike}Z`)
  if (!Number.isNaN(asUtc.getTime())) return asUtc

  const match = normalized.match(
    /^(\d{2})[-/](\d{2})[-/](\d{4})(?:[ T](\d{2}):(\d{2})(?::(\d{2}))?)?$/
  )
  if (match) {
    const [, day, month, year, hour = '00', minute = '00', second = '00'] = match
    return new Date(Date.UTC(Number(year), Number(month) - 1, Number(day), Number(hour), Number(minute), Number(second)))
  }

  return new Date(normalized)
}

export function formatIstDateTime(
  timestamp: string | number | Date | null | undefined,
  options: Intl.DateTimeFormatOptions = {}
): string {
  if (timestamp === null || timestamp === undefined || timestamp === '') return '-'
  const date = parseIstTimestamp(timestamp)
  if (Number.isNaN(date.getTime())) return '-'
  return new Intl.DateTimeFormat('en-IN', {
    timeZone: IST_TIME_ZONE,
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: true,
    ...options,
  }).format(date)
}

export function formatIstDate(timestamp: string | number | Date | null | undefined): string {
  if (timestamp === null || timestamp === undefined || timestamp === '') return '-'
  const date = parseIstTimestamp(timestamp)
  if (Number.isNaN(date.getTime())) return '-'
  return new Intl.DateTimeFormat('en-IN', {
    timeZone: IST_TIME_ZONE,
    year: 'numeric',
    month: 'short',
    day: '2-digit',
  }).format(date)
}

export function formatIstTime(timestamp: string | number | Date | null | undefined): string {
  if (timestamp === null || timestamp === undefined || timestamp === '') return '-'
  const date = parseIstTimestamp(timestamp)
  if (Number.isNaN(date.getTime())) return '-'
  return new Intl.DateTimeFormat('en-IN', {
    timeZone: IST_TIME_ZONE,
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: true,
  }).format(date)
}

export function formatIstDateTimeFromUtc(
  timestamp: string | number | Date | null | undefined,
  options: Intl.DateTimeFormatOptions = {}
): string {
  if (timestamp === null || timestamp === undefined || timestamp === '') return '-'
  const date = parseUtcTimestamp(timestamp)
  if (Number.isNaN(date.getTime())) return '-'
  return new Intl.DateTimeFormat('en-IN', {
    timeZone: IST_TIME_ZONE,
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: true,
    ...options,
  }).format(date)
}

export function formatIstTimeFromUtc(timestamp: string | number | Date | null | undefined): string {
  if (timestamp === null || timestamp === undefined || timestamp === '') return '-'
  const date = parseUtcTimestamp(timestamp)
  if (Number.isNaN(date.getTime())) return '-'
  return new Intl.DateTimeFormat('en-IN', {
    timeZone: IST_TIME_ZONE,
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: true,
  }).format(date)
}

export function todayIstIsoDate(): string {
  const map = buildPartsMap(new Date())
  return `${map.year}-${map.month}-${map.day}`
}

export function nowIstIso(): string {
  const map = buildPartsMap(new Date())
  return `${map.year}-${map.month}-${map.day}T${map.hour}:${map.minute}:${map.second}+05:30`
}

export function toIstIso(timestamp: string | number | Date | null | undefined): string | null {
  if (timestamp === null || timestamp === undefined || timestamp === '') return null
  const date = parseIstTimestamp(timestamp)
  if (Number.isNaN(date.getTime())) return null
  const map = buildPartsMap(date)
  return `${map.year}-${map.month}-${map.day}T${map.hour}:${map.minute}:${map.second}+05:30`
}

export function istDateInputValue(timestamp: string | number | Date | null | undefined): string {
  if (timestamp === null || timestamp === undefined || timestamp === '') return ''
  const date = parseIstTimestamp(timestamp)
  if (Number.isNaN(date.getTime())) return ''
  const map = buildPartsMap(date)
  return `${map.year}-${map.month}-${map.day}`
}

export function istOffsetMinutes(): number {
  return IST_OFFSET_MINUTES
}

export function shiftIstDate(days = 0, months = 0): string {
  const map = buildPartsMap(new Date())
  const base = new Date(Date.UTC(Number(map.year), Number(map.month) - 1, Number(map.day)))
  base.setUTCDate(base.getUTCDate() + days)
  base.setUTCMonth(base.getUTCMonth() + months)
  return `${base.getUTCFullYear()}-${pad(base.getUTCMonth() + 1)}-${pad(base.getUTCDate())}`
}
