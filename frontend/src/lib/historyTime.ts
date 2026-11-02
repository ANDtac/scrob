/** Occurrence precision is source evidence, not the import/observation date. */
export interface Occurred {
  precision: "legacy" | "instant" | "date" | "local_datetime" | "unknown";
  value?: string | null;
  timezone?: string | null;
  raw?: string | null;
}

export interface WatchTime {
  watched_at: string | null;
  watched_precision?: Occurred["precision"];
  watched_on?: string | null;
  watched_local_at?: string | null;
  watched_timezone?: string | null;
}

export function watchOccurrence(event: WatchTime): Occurred {
  const precision = event.watched_precision ?? "legacy";
  const value = precision === "date" ? event.watched_on
    : precision === "local_datetime" ? event.watched_local_at : event.watched_at;
  return { precision, value, timezone: event.watched_timezone };
}

function calendarDate(value: string): Date | null {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return null;
  const date = new Date(`${value}T12:00:00Z`);
  return Number.isFinite(date.getTime()) && date.toISOString().slice(0, 10) === value ? date : null;
}

export function formatOccurrence(occurred: Occurred | null | undefined, options: { timeZone?: string; use24h?: boolean } = {}) {
  const unknown = { dateLabel: "Date unknown", timeLabel: "Time unknown", fullLabel: "Date unknown", canOrder: false };
  if (!occurred || occurred.precision === "unknown" || !occurred.value) return unknown;
  const value = occurred.value;
  const dateOptions: Intl.DateTimeFormatOptions = { weekday: "long", month: "long", day: "numeric", year: "numeric" };
  if (occurred.precision === "date" || occurred.precision === "local_datetime") {
    const date = calendarDate(value.slice(0, 10));
    if (!date || (occurred.precision === "date" && value.length !== 10)) return unknown;
    const dateLabel = date.toLocaleDateString("en-US", { ...dateOptions, timeZone: "UTC" });
    if (occurred.precision === "date") {
      return { dateLabel, timeLabel: "Time unknown · date only", fullLabel: `${dateLabel} (date only)`, canOrder: false };
    }
    const clock = /^\d{4}-\d{2}-\d{2}T([01]\d|2[0-3]):([0-5]\d)(?::[0-5]\d(?:\.\d+)?)?$/.exec(value);
    if (!clock) return unknown;
    const zone = occurred.timezone ? `local time, ${occurred.timezone}` : "local time, timezone unknown";
    const timeLabel = `${clock[1]}:${clock[2]} (${zone})`;
    return { dateLabel: `${dateLabel} (${zone})`, timeLabel, fullLabel: `${dateLabel}, ${timeLabel}`, canOrder: false };
  }
  // Legacy Scrob dates and the canonical instant projection are UTC without
  // an offset. Preserve explicit offsets when present; never append a second Z.
  const instant = new Date(/(?:Z|[+-]\d{2}:\d{2})$/i.test(value) ? value : `${value}Z`);
  if (!Number.isFinite(instant.getTime())) return unknown;
  let timeZone = options.timeZone;
  try { new Intl.DateTimeFormat("en-US", { timeZone }); } catch { timeZone = "UTC"; }
  const dateLabel = instant.toLocaleDateString("en-US", { ...dateOptions, timeZone });
  const timeLabel = instant.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", hour12: !options.use24h, timeZone });
  return { dateLabel, timeLabel, fullLabel: `${dateLabel}, ${timeLabel}`, canOrder: true };
}
