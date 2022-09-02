import { formatTimeoutString } from './utils';

it('formats a timeout string', () => {
  expect(formatTimeoutString('1d1h1m1s')).toEqual('1 day 1 hr 1 min 1 sec');
  expect(formatTimeoutString('11d11h11m11s')).toEqual('11 days 11 hrs 11 mins 11 secs');
  expect(formatTimeoutString('0d0h0m0s')).toEqual('0d0h0m0s');
  expect(formatTimeoutString('1d0h11m0s')).toEqual('1 day 11 mins');
});

it('returns original value if invalid unit', () => {
  expect(formatTimeoutString('1w')).toEqual('1w');
});
