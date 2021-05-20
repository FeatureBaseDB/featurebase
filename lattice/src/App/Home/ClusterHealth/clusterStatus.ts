type clusterStatuses = 'NORMAL' | 'DEGRADED' | 'STARTING' | 'RESIZING' | 'DOWN' | 'UNKNOWN';

export const CLUSTER_STATUS: {
  [key in clusterStatuses]: { label: string; status: string };
} = {
  NORMAL: {
    label: 'All nodes are up, cluster is healthy.',
    status: 'success'
  },
  DEGRADED: {
    label:
      'Some nodes are down but all data is available and queries can still be answered, but performance may be worse.',
    status: 'warning'
  },
  STARTING: {
    label: 'Some nodes are up, but not enough to answer queries.',
    status: 'error'
  },
  RESIZING: {
    label:
      'Cluster is resizing. Most endpoints are unavailable until the resize completes.',
    status: 'info'
  },
  DOWN: {
    label: 'Cluster is unable to serve queries.',
    status: 'disabled'
  },
  UNKNOWN: {
    label: 'Unable to get cluster status.',
    status: 'disabled'
  }
};
