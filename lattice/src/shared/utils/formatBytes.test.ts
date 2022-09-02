import { formatBytes } from './formatBytes';

it('formats bytes to appropriate unit', () => {
  expect(formatBytes(0)).toEqual('0 Bytes');
  expect(formatBytes(Math.pow(1024, 1))).toEqual('1 KB');
  expect(formatBytes(Math.pow(1024, 2))).toEqual('1 MB');
  expect(formatBytes(Math.pow(1024, 3))).toEqual('1 GB');
  expect(formatBytes(Math.pow(1024, 4))).toEqual('1 TB');
  expect(formatBytes(Math.pow(1024, 5))).toEqual('1 PB');
  expect(formatBytes(Math.pow(1024, 6))).toEqual('1 EB');
  expect(formatBytes(Math.pow(1024, 7))).toEqual('1 ZB');
  expect(formatBytes(Math.pow(1024, 8))).toEqual('1 YB');
});

it('formats > 1024 YB in terms of YBs', () => {
  expect(formatBytes(Math.pow(1024, 10))).toEqual('1048576 YB');
})

it('returns 0 if given value is negative', () => {
  expect(formatBytes(-1)).toEqual('0 Bytes');
});
