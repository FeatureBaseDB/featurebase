export const operators = {
  decimal: [
    { label: '>', value: '>' },
    { label: '<', value: '<' },
    { label: '>=', value: '>=' },
    { label: '<=', value: '<=' },
    { label: '==', value: '=' },
    { label: '!=', value: '!=' },
  ],
  int: [
    { label: '>', value: '>' },
    { label: '<', value: '<' },
    { label: '>=', value: '>=' },
    { label: '<=', value: '<=' },
    { label: '==', value: '=' },
    { label: '!=', value: '!=' },
  ],
  'mutex-id': [
    { label: 'is', value: '=' },
    { label: 'is not', value: '!=' }
  ],
  'mutex-keys': [
    { label: 'is', value: '=' },
    { label: 'is not', value: '!=' },
    { label: 'like', value: 'like' },
    { label: 'CIDR', value: 'cidr' }
  ],
  "set-id": [
    { label: 'is', value: '=' },
    { label: 'is not', value: '!=' }
  ],
  "set-keys": [
    { label: 'is', value: '=' },
    { label: 'is not', value: '!=' },
    { label: 'like', value: 'like' },
    { label: 'CIDR', value: 'cidr' }
  ],
  "time-id": [
    { label: 'is', value: '=' },
    { label: 'is not', value: '!=' }
  ],
  "time-keys": [
    { label: 'is', value: '=' },
    { label: 'is not', value: '!=' },
    { label: 'like', value: 'like' }
  ],
  timestamp: [
    { label: 'is before', value: '<' },
    { label: 'is after', value: '>' },
    { label: 'is', value: '=' },
  ]
}
