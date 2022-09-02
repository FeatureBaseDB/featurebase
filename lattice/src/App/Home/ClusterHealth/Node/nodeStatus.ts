type nodeStates = 'STARTING' | 'STARTED' | 'RESIZING' | 'UNKNOWN' | 'READY' | 'DOWN';

export const NODE_STATE: {
  [key in nodeStates]: { label: string; status: string };
} = {
  STARTING: {
    label: '',
    status: 'info'
  },
  STARTED: {
    label: '',
    status: 'success'
  },
  RESIZING: {
    label: '',
    status: 'warning'
  },
  UNKNOWN: {
    label: 'Unable to get node status',
    status: 'disabled'
  },
  // deprecated
  READY: {
    label: '',
    status: 'success'
  },
  DOWN: {
    label: '',
    status: 'disabled'
  }
}
