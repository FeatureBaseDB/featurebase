export const nodeInfo = [
  { name: 'shardWidth', tooltip: 'Number of records in a shard, which affects query performance (concurrency): maxConcurrency = ShardCount = RecordCount/ShardWidth.' },
  { name: 'replicaN', tooltip: 'Total number of data copies that are distributed around the cluster. Higher values allow reads to continue with some node failures, lower values decrease data footprint.' },
  { name: 'cpuType', tooltip: '' },
  { name: 'cpuPhysicalCores', tooltip: '' },
  { name: 'cpuLogicalCores', tooltip: '' },
  { name: 'cpuMHz', tooltip: '' },
  { name: 'txSrc', tooltip: 'Storage engine for bitmap data.' },
  { name: 'version', tooltip: '' }
];
