export const operators = {
  int: [
    { label: '>', value: '>' },
    { label: '<', value: '<' },
    { label: '>=', value: '>=' },
    { label: '<=', value: '<=' },
    { label: '==', value: '=' },
    { label: '!=', value: '!=' },
  ],
  set: [
    { label: 'is', value: '=' },
    { label: 'is not', value: '!=' },
    { label: 'like', value: 'like' },
    { label: 'CIDR', value: 'cidr' }
  ],
  timestamp: [
    { label: 'is before', value: '<' },
    { label: 'is after', value: '>' },
    { label: 'is', value: '=' },
  ]
}
