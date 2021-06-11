import { stringifyRowData } from './stringifyRowData';

describe('decimal types', () => {
  it('stringifies = operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "=",
        "value": "0",
        "type": "decimal",
        "keys": undefined
      }]
    }])).toEqual({ error: false, query: 'Row(fieldName=0)' })
  });

  it('stringifies != operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "!=",
        "value": "0",
        "type": "decimal",
        "keys": undefined
      }]
    }])).toEqual({ error: false, query: 'Not(Row(fieldName=0))' })
  });

  it('stringifies > operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": ">",
        "value": "0",
        "type": "decimal",
        "keys": undefined
      }]
    }])).toEqual({ error: false, query: 'Row(fieldName>0)' })
  });

  it('stringifies >= operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": ">=",
        "value": "0",
        "type": "decimal",
        "keys": undefined
      }]
    }])).toEqual({ error: false, query: 'Row(fieldName>=0)' })
  });

  it('stringifies negated operations', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": ">=",
        "value": "0",
        "type": "decimal",
        "keys": undefined
      }],
      isNot: true
    }])).toEqual({ error: false, query: 'Not(Row(fieldName>=0))' })
  });
});

describe('int types', () => {
  it('stringifies = operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "=",
        "value": "0",
        "type": "int",
        "keys": undefined
      }]
    }])).toEqual({ error: false, query: 'Row(fieldName=0)' })
  });

  it('stringifies != operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "!=",
        "value": "0",
        "type": "int",
        "keys": undefined
      }]
    }])).toEqual({ error: false, query: 'Not(Row(fieldName=0))' })
  });

  it('stringifies > operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": ">",
        "value": "0",
        "type": "int",
        "keys": undefined
      }]
    }])).toEqual({ error: false, query: 'Row(fieldName>0)' })
  });

  it('stringifies >= operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": ">=",
        "value": "0",
        "type": "int",
        "keys": undefined
      }]
    }])).toEqual({ error: false, query: 'Row(fieldName>=0)' })
  });

  it('stringifies negated operations', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": ">=",
        "value": "0",
        "type": "int",
        "keys": undefined
      }],
      isNot: true
    }])).toEqual({ error: false, query: 'Not(Row(fieldName>=0))' })
  });
});

describe('mutex (id) types', () => {
  it('stringifies is operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "=",
        "value": "1234",
        "type": "mutex",
        "keys": undefined
      }]
    }])).toEqual({ error: false, query: 'Row(fieldName=1234)' })
  });

  it('stringifies is operator for multiple values', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "=",
        "value": "1234, 5678",
        "type": "mutex",
        "keys": undefined
      }]
    }])).toEqual({ error: false, query: 'Union(Row(fieldName=1234), Row(fieldName=5678))' })
  });

  it('stringifies is not operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "!=",
        "value": "1234",
        "type": "mutex",
        "keys": undefined
      }]
    }])).toEqual({ error: false, query: 'Not(Row(fieldName=1234))' })
  });

  it('stringifies is not operator for multiple values', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "!=",
        "value": "1234, 5678",
        "type": "mutex",
        "keys": undefined
      }]
    }])).toEqual({ error: false, query: 'Not(Union(Row(fieldName=1234), Row(fieldName=5678)))' })
  });
});

describe('mutex (keys) types', () => {
  it('stringifies is operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "=",
        "value": "value",
        "type": "mutex",
        "keys": true
      }]
    }])).toEqual({ error: false, query: 'Row(fieldName="value")' })
  });

  it('stringifies is operator for multiple values', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "=",
        "value": "one, two",
        "type": "mutex",
        "keys": true
      }]
    }])).toEqual({ error: false, query: 'Union(Row(fieldName="one"), Row(fieldName="two"))' })
  });

  it('stringifies is not operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "!=",
        "value": "value",
        "type": "mutex",
        "keys": true
      }]
    }])).toEqual({ error: false, query: 'Not(Row(fieldName="value"))' })
  });

  it('stringifies is not operator for multiple values', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "!=",
        "value": "one, two",
        "type": "mutex",
        "keys": true
      }]
    }])).toEqual({ error: false, query: 'Not(Union(Row(fieldName="one"), Row(fieldName="two")))' })
  });

  it('stringifies like operator with a single-codepoint key', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "like",
        "value": "_alue",
        "type": "mutex",
        "keys": true
      }]
    }])).toEqual({ error: false, query: 'UnionRows(Rows(field=fieldName, like="_alue"))' })
  });

  it('stringifies like operator with a wildcard', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "like",
        "value": "value%",
        "type": "mutex",
        "keys": true
      }]
    }])).toEqual({ error: false, query: 'UnionRows(Rows(field=fieldName, like="value%"))' })
  });

  it('stringifies like operator without a single-codepoint key or wildcard', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "like",
        "value": "value",
        "type": "mutex",
        "keys": true
      }]
    }])).toEqual({ error: false, query: 'UnionRows(Rows(field=fieldName, like="%value%"))' })
  });

  it('stringifies CIDR operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "cidr",
        "value": "10.164.124.33/30",
        "type": "mutex",
        "keys": true
      }]
    }])).toEqual({ error: false, query: 'Union(Row(fieldName="10.164.124.32"), Row(fieldName="10.164.124.33"), Row(fieldName="10.164.124.34"), Row(fieldName="10.164.124.35"))' })
  });

  it('stringifies negated operations', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "like",
        "value": "value",
        "type": "mutex",
        "keys": true
      }],
      isNot: true
    }])).toEqual({ error: false, query: 'Not(UnionRows(Rows(field=fieldName, like="%value%")))' })
  });
});

describe('set (id) types', () => {
  it('stringifies is operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "=",
        "value": "1234",
        "type": "set",
        "keys": undefined
      }]
    }])).toEqual({ error: false, query: 'Row(fieldName=1234)' })
  });

  it('stringifies is operator for multiple values', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "=",
        "value": "1234, 5678",
        "type": "set",
        "keys": undefined
      }]
    }])).toEqual({ error: false, query: 'Union(Row(fieldName=1234), Row(fieldName=5678))' })
  });

  it('stringifies is not operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "!=",
        "value": "1234",
        "type": "set",
        "keys": undefined
      }]
    }])).toEqual({ error: false, query: 'Not(Row(fieldName=1234))' })
  });

  it('stringifies is not operator for multiple values', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "!=",
        "value": "1234, 5678",
        "type": "set",
        "keys": undefined
      }]
    }])).toEqual({ error: false, query: 'Not(Union(Row(fieldName=1234), Row(fieldName=5678)))' })
  });
});

describe('set (keys) types', () => {
  it('stringifies is operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "=",
        "value": "value",
        "type": "set",
        "keys": true
      }]
    }])).toEqual({ error: false, query: 'Row(fieldName="value")' })
  });

  it('stringifies is operator for multiple values', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "=",
        "value": "one, two",
        "type": "set",
        "keys": true
      }]
    }])).toEqual({ error: false, query: 'Union(Row(fieldName="one"), Row(fieldName="two"))' })
  });

  it('stringifies is not operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "!=",
        "value": "value",
        "type": "set",
        "keys": true
      }]
    }])).toEqual({ error: false, query: 'Not(Row(fieldName="value"))' })
  });

  it('stringifies is not operator for multiple values', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "!=",
        "value": "one, two",
        "type": "set",
        "keys": true
      }]
    }])).toEqual({ error: false, query: 'Not(Union(Row(fieldName="one"), Row(fieldName="two")))' })
  });

  it('stringifies like operator with a single-codepoint key', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "like",
        "value": "_alue",
        "type": "set",
        "keys": true
      }]
    }])).toEqual({ error: false, query: 'UnionRows(Rows(field=fieldName, like="_alue"))' })
  });

  it('stringifies like operator with a wildcard', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "like",
        "value": "value%",
        "type": "set",
        "keys": true
      }]
    }])).toEqual({ error: false, query: 'UnionRows(Rows(field=fieldName, like="value%"))' })
  });

  it('stringifies like operator without a single-codepoint key or wildcard', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "like",
        "value": "value",
        "type": "set",
        "keys": true
      }]
    }])).toEqual({ error: false, query: 'UnionRows(Rows(field=fieldName, like="%value%"))' })
  });

  it('stringifies CIDR operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "cidr",
        "value": "10.164.124.33/30",
        "type": "set",
        "keys": true
      }]
    }])).toEqual({ error: false, query: 'Union(Row(fieldName="10.164.124.32"), Row(fieldName="10.164.124.33"), Row(fieldName="10.164.124.34"), Row(fieldName="10.164.124.35"))' })
  });

  it('stringifies negated operations', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "like",
        "value": "value",
        "type": "set",
        "keys": true
      }],
      isNot: true
    }])).toEqual({ error: false, query: 'Not(UnionRows(Rows(field=fieldName, like="%value%")))' })
  });
});

describe('time (ID) types', () => {
  it('stringifies is operators', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "=",
        "value": "1234",
        "type": "time",
        "keys": undefined
      }]
    }])).toEqual({ error: false, query: 'Row(fieldName=1234)' })
  });

  it('stringifies is not operators', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "!=",
        "value": "1234",
        "type": "time",
        "keys": undefined
      }]
    }])).toEqual({ error: false, query: 'Not(Row(fieldName=1234))' })
  });

  it('stringifies negated operators', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "=",
        "value": "1234",
        "type": "time",
        "keys": undefined
      }],
      isNot: true
    }])).toEqual({ error: false, query: 'Not(Row(fieldName=1234))' })
  });
});

describe('time (keys) types', () => {
  it('stringifies is operators', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "=",
        "value": "1234",
        "type": "time",
        "keys": true
      }]
    }])).toEqual({ error: false, query: 'Row(fieldName="1234")' })
  });

  it('stringifies is not operators', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "!=",
        "value": "1234",
        "type": "time",
        "keys": true
      }]
    }])).toEqual({ error: false, query: 'Not(Row(fieldName="1234"))' })
  });

  it('stringifies like operators with a single-codepoint key', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "like",
        "value": "123_",
        "type": "time",
        "keys": true
      }]
    }])).toEqual({ error: false, query: 'UnionRows(Rows(field=fieldName, like="123_"))' })
  });

  it('stringifies like operators with a wildcard', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "like",
        "value": "%1234",
        "type": "time",
        "keys": true
      }]
    }])).toEqual({ error: false, query: 'UnionRows(Rows(field=fieldName, like="%1234"))' })
  });

  it('stringifies like operators without a single-codepoint key or wildcard', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "like",
        "value": "1234",
        "type": "time",
        "keys": true
      }]
    }])).toEqual({ error: false, query: 'UnionRows(Rows(field=fieldName, like="%1234%"))' })
  });

  it('stringifies negated operators', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "=",
        "value": "1234",
        "type": "time",
        "keys": true
      }],
      isNot: true
    }])).toEqual({ error: false, query: 'Not(Row(fieldName="1234"))' })
  });
});

describe('timestamp types', () => {
  it('stringifies is operators', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "=",
        "value": "2021-06-04T12:00:00Z",
        "type": "timestamp",
        "keys": undefined
      }],
    }])).toEqual({ error: false, query: 'Row(fieldName="2021-06-04T12:00:00Z")' })
  });

  it('stringifies is before operators', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "<",
        "value": "2021-06-04T12:00:00Z",
        "type": "timestamp",
        "keys": undefined
      }],
    }])).toEqual({ error: false, query: 'Row(fieldName<"2021-06-04T12:00:00Z")' })
  });

  it('stringifies is after operators', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": ">",
        "value": "2021-06-04T12:00:00Z",
        "type": "timestamp",
        "keys": undefined
      }],
    }])).toEqual({ error: false, query: 'Row(fieldName>"2021-06-04T12:00:00Z")' })
  });

  it('stringifies negated operators', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "=",
        "value": "2021-06-04T12:00:00Z",
        "type": "timestamp",
        "keys": undefined
      }],
      isNot: true
    }])).toEqual({ error: false, query: 'Not(Row(fieldName="2021-06-04T12:00:00Z"))' })
  });
});

describe('mixed types', () => {
  it('stringifies multiple and-ed rows', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "stringType",
        "rowOperator": "like",
        "value": "value",
        "type": "set",
        "keys": true
      }, {
        "field": "intType",
        "rowOperator": "=",
        "value": "0",
        "type": "int",
        "keys": undefined
      }],
      operator: "and"
    }])).toEqual({ error: false, query: 'Intersect(UnionRows(Rows(field=stringType, like="%value%")), Row(intType=0))' })
  });

  it('stringifies multiple or-ed rows', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "stringType",
        "rowOperator": "like",
        "value": "value",
        "type": "set",
        "keys": true
      }, {
        "field": "intType",
        "rowOperator": "=",
        "value": "0",
        "type": "int",
        "keys": undefined
      }],
      operator: "or"
    }])).toEqual({ error: false, query: 'Union(UnionRows(Rows(field=stringType, like="%value%")), Row(intType=0))' })
  });

  it('stringifies negated multiple rows', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "stringType",
        "rowOperator": "like",
        "value": "value",
        "type": "set",
        "keys": true
      }, {
        "field": "intType",
        "rowOperator": "=",
        "value": "0",
        "type": "int",
        "keys": undefined
      }],
      operator: "or",
      isNot: true
    }])).toEqual({ error: false, query: 'Not(Union(UnionRows(Rows(field=stringType, like="%value%")), Row(intType=0)))' })
  });
})
