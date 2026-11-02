import test from 'node:test';
import assert from 'node:assert/strict';
import { formatOccurrence, watchOccurrence } from './historyTime.ts';

test('date-only stays on the source day in opposite viewer timezones', () => {
  const occurred = { precision: 'date', value: '2020-01-01' };
  for (const timeZone of ['America/Los_Angeles', 'Pacific/Kiritimati']) {
    const result = formatOccurrence(occurred, { timeZone });
    assert.match(result.dateLabel, /January 1, 2020/);
    assert.match(result.timeLabel, /date only/);
    assert.equal(result.canOrder, false);
  }
});
test('unknown never displays a legacy placeholder or observation instant', () => {
  const result = formatOccurrence(watchOccurrence({ watched_precision: 'unknown', watched_at: '2026-11-01T10:00:00' }));
  assert.equal(result.fullLabel, 'Date unknown');
});
test('local datetime preserves the wall clock and marks unknown timezone', () => {
  const result = formatOccurrence({ precision: 'local_datetime', value: '2020-01-01T00:15:23' }, { timeZone: 'America/Los_Angeles' });
  assert.match(result.dateLabel, /January 1, 2020/);
  assert.equal(result.timeLabel, '00:15 (local time, timezone unknown)');
  assert.equal(result.canOrder, false);
});
test('explicit offsets and canonical naive UTC project to the same instant', () => {
  const options = { timeZone: 'America/New_York', use24h: true };
  const a = formatOccurrence({ precision: 'instant', value: '2020-01-01T02:15:00Z' }, options);
  const b = formatOccurrence({ precision: 'instant', value: '2019-12-31T21:15:00-05:00' }, options);
  const legacy = formatOccurrence(watchOccurrence({ watched_at: '2020-01-01T02:15:00' }), options);
  assert.deepEqual(a, b);
  assert.deepEqual(a, legacy);
  assert.match(a.dateLabel, /December 31, 2019/);
  assert.equal(a.timeLabel, '21:15');
});
test('invalid temporal evidence is not rendered as a valid date', () => {
  for (const occurred of [null, { precision: 'date', value: '2021-02-29' }, { precision: 'local_datetime', value: '2020-01-01T25:30:00' }, { precision: 'instant', value: 'invalid' }]) {
    assert.equal(formatOccurrence(occurred).fullLabel, 'Date unknown');
  }
});
