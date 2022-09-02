import { formatDuration } from './formatDuration';

it('formats duration', () => {
  expect(formatDuration(-1)).toEqual('0 ms');
  expect(formatDuration(0)).toEqual('0 ms');
  expect(formatDuration(250)).toEqual('250 ms');
  expect(formatDuration(251)).toEqual('0.25 sec');
});

it('formats duration (in nanoseconds)', () => {
  expect(formatDuration(-1, true)).toEqual('0 ms');
  expect(formatDuration(0, true)).toEqual('0 ms');
  expect(formatDuration(1000000, true)).toEqual('1 ms');
  expect(formatDuration(250000000, true)).toEqual('250 ms');
  expect(formatDuration(250000001, true)).toEqual('0.25 sec');
});
