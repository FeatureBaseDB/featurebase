import {
  stringifyRowData,
  stringifyExtract,
  stringifyCount,
  stringifyGroupBy
} from './utils';

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
    }])).toEqual({ error: false, queryString: 'Row(fieldName=0)' })
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
    }])).toEqual({ error: false, queryString: 'Not(Row(fieldName=0))' })
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
    }])).toEqual({ error: false, queryString: 'Row(fieldName>0)' })
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
    }])).toEqual({ error: false, queryString: 'Row(fieldName>=0)' })
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
    }])).toEqual({ error: false, queryString: 'Not(Row(fieldName>=0))' })
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
    }])).toEqual({ error: false, queryString: 'Row(fieldName=0)' })
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
    }])).toEqual({ error: false, queryString: 'Not(Row(fieldName=0))' })
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
    }])).toEqual({ error: false, queryString: 'Row(fieldName>0)' })
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
    }])).toEqual({ error: false, queryString: 'Row(fieldName>=0)' })
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
    }])).toEqual({ error: false, queryString: 'Not(Row(fieldName>=0))' })
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
    }])).toEqual({ error: false, queryString: 'Row(fieldName=1234)' })
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
    }])).toEqual({ error: false, queryString: 'Union(Row(fieldName=1234), Row(fieldName=5678))' })
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
    }])).toEqual({ error: false, queryString: 'Not(Row(fieldName=1234))' })
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
    }])).toEqual({ error: false, queryString: 'Not(Union(Row(fieldName=1234), Row(fieldName=5678)))' })
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
    }])).toEqual({ error: false, queryString: 'Row(fieldName="value")' })
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
    }])).toEqual({ error: false, queryString: 'Union(Row(fieldName="one"), Row(fieldName="two"))' })
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
    }])).toEqual({ error: false, queryString: 'Not(Row(fieldName="value"))' })
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
    }])).toEqual({ error: false, queryString: 'Not(Union(Row(fieldName="one"), Row(fieldName="two")))' })
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
    }])).toEqual({ error: false, queryString: 'UnionRows(Rows(field=fieldName, like="_alue"))' })
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
    }])).toEqual({ error: false, queryString: 'UnionRows(Rows(field=fieldName, like="value%"))' })
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
    }])).toEqual({ error: false, queryString: 'UnionRows(Rows(field=fieldName, like="%value%"))' })
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
    }])).toEqual({ error: false, queryString: 'Union(Row(fieldName="10.164.124.32"), Row(fieldName="10.164.124.33"), Row(fieldName="10.164.124.34"), Row(fieldName="10.164.124.35"))' })
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
    }])).toEqual({ error: false, queryString: 'Not(UnionRows(Rows(field=fieldName, like="%value%")))' })
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
    }])).toEqual({ error: false, queryString: 'Row(fieldName=1234)' })
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
    }])).toEqual({ error: false, queryString: 'Union(Row(fieldName=1234), Row(fieldName=5678))' })
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
    }])).toEqual({ error: false, queryString: 'Not(Row(fieldName=1234))' })
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
    }])).toEqual({ error: false, queryString: 'Not(Union(Row(fieldName=1234), Row(fieldName=5678)))' })
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
    }])).toEqual({ error: false, queryString: 'Row(fieldName="value")' })
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
    }])).toEqual({ error: false, queryString: 'Union(Row(fieldName="one"), Row(fieldName="two"))' })
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
    }])).toEqual({ error: false, queryString: 'Not(Row(fieldName="value"))' })
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
    }])).toEqual({ error: false, queryString: 'Not(Union(Row(fieldName="one"), Row(fieldName="two")))' })
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
    }])).toEqual({ error: false, queryString: 'UnionRows(Rows(field=fieldName, like="_alue"))' })
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
    }])).toEqual({ error: false, queryString: 'UnionRows(Rows(field=fieldName, like="value%"))' })
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
    }])).toEqual({ error: false, queryString: 'UnionRows(Rows(field=fieldName, like="%value%"))' })
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
    }])).toEqual({ error: false, queryString: 'Union(Row(fieldName="10.164.124.32"), Row(fieldName="10.164.124.33"), Row(fieldName="10.164.124.34"), Row(fieldName="10.164.124.35"))' })
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
    }])).toEqual({ error: false, queryString: 'Not(UnionRows(Rows(field=fieldName, like="%value%")))' })
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
    }])).toEqual({ error: false, queryString: 'Row(fieldName=1234)' })
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
    }])).toEqual({ error: false, queryString: 'Not(Row(fieldName=1234))' })
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
    }])).toEqual({ error: false, queryString: 'Not(Row(fieldName=1234))' })
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
    }])).toEqual({ error: false, queryString: 'Row(fieldName="1234")' })
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
    }])).toEqual({ error: false, queryString: 'Not(Row(fieldName="1234"))' })
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
    }])).toEqual({ error: false, queryString: 'UnionRows(Rows(field=fieldName, like="123_"))' })
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
    }])).toEqual({ error: false, queryString: 'UnionRows(Rows(field=fieldName, like="%1234"))' })
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
    }])).toEqual({ error: false, queryString: 'UnionRows(Rows(field=fieldName, like="%1234%"))' })
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
    }])).toEqual({ error: false, queryString: 'Not(Row(fieldName="1234"))' })
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
    }])).toEqual({ error: false, queryString: 'Row(fieldName="2021-06-04T12:00:00Z")' })
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
    }])).toEqual({ error: false, queryString: 'Row(fieldName<"2021-06-04T12:00:00Z")' })
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
    }])).toEqual({ error: false, queryString: 'Row(fieldName>"2021-06-04T12:00:00Z")' })
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
    }])).toEqual({ error: false, queryString: 'Not(Row(fieldName="2021-06-04T12:00:00Z"))' })
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
    }])).toEqual({ error: false, queryString: 'Intersect(UnionRows(Rows(field=stringType, like="%value%")), Row(intType=0))' })
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
    }])).toEqual({ error: false, queryString: 'Union(UnionRows(Rows(field=stringType, like="%value%")), Row(intType=0))' })
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
    }])).toEqual({ error: false, queryString: 'Not(Union(UnionRows(Rows(field=stringType, like="%value%")), Row(intType=0)))' })
  });
})

describe('Query types', () => {
  it('stringifies Extract queries', () => {
    expect(stringifyExtract({
      columns: ['one', 'two', 'three'],
      rowCalls: [{
        row: [{
          "field": "stringType",
          "rowOperator": "like",
          "value": "value",
          "type": "set",
          "keys": true
        }, {
          "field": "stringType2",
          "rowOperator": "is",
          "value": "value2",
          "type": "set",
          "keys": true
        }],
        operator: 'or'
      }, {
        row: [{
          "field": "intType",
          "rowOperator": "=",
          "value": "0",
          "type": "int",
          "keys": undefined
        }]
      }],
      operator: "and"
    })).toEqual({ error: false, queryString: 'Extract(Limit(Intersect(Union(UnionRows(Rows(field=stringType, like=\"%value%\")), Row(stringType2is\"value2\")), Row(intType=0)), limit=1000), Rows(one), Rows(two), Rows(three))', countQuery: 'Count(Intersect(Union(UnionRows(Rows(field=stringType, like=\"%value%\")), Row(stringType2is\"value2\")), Row(intType=0)))' })
  });

  it('stringifies Count queries', () => {
    expect(stringifyCount({
      columns: ['one', 'two', 'three'],
      rowCalls: [{
        row: [{
          "field": "stringType",
          "rowOperator": "like",
          "value": "value",
          "type": "set",
          "keys": true
        }, {
          "field": "stringType2",
          "rowOperator": "is",
          "value": "value2",
          "type": "set",
          "keys": true
        }],
        operator: 'or'
      }, {
        row: [{
          "field": "intType",
          "rowOperator": "=",
          "value": "0",
          "type": "int",
          "keys": undefined
        }]
      }],
      operator: "and"
    })).toEqual({ error: false, queryString: 'Count(Intersect(Union(UnionRows(Rows(field=stringType, like=\"%value%\")), Row(stringType2is\"value2\")), Row(intType=0)))' })
  });

  it('stringifies basic GroupBy queries', () => {
    expect(stringifyGroupBy({
      groupByCall: {
        primary: 'primary'
      }
    })).toEqual('GroupBy(Rows(primary))');
  });

  it('stringifies GroupBy queries with secondary grouping', () => {
    expect(stringifyGroupBy({
      groupByCall: {
        primary: 'primary',
        secondary: 'secondary'
      }
    })).toEqual('GroupBy(Rows(primary), Rows(secondary))');
  });

  it('stringifies GroupBy queries with a filter', () => {
    expect(stringifyGroupBy({
      groupByCall: {
        primary: 'primary'
      },
      filter: 'filterString'
    })).toEqual('GroupBy(Rows(primary), filter=filterString)');
  });

  it('stringifies GroupBy queries with a sort', () => {
    expect(stringifyGroupBy({
      groupByCall: {
        primary: 'primary'
      },
      sort: [{ sortValue: 'count desc' }]
    })).toEqual('GroupBy(Rows(primary), sort=\"count desc\")');
  });

  it('stringifies GroupBy queries with a secondary sum sort', () => {
    expect(stringifyGroupBy({
      groupByCall: {
        primary: 'primary'
      },
      sort: [
        { sortValue: 'count desc' },
        { sortValue: 'sum asc', field: 'field' }
      ]
    })).toEqual('GroupBy(Rows(primary), sort=\"count desc, sum asc\", aggregate=Sum(field=field))');
  });

  it('stringifies GroupBy queries with a filter and sort', () => {
    expect(stringifyGroupBy({
      groupByCall: {
        primary: 'primary'
      },
      filter: 'filterString',
      sort: [
        { sortValue: 'sum asc', field: 'field' }
      ]
    })).toEqual('GroupBy(Rows(primary), filter=filterString, sort=\"sum asc\", aggregate=Sum(field=field))');
  });
});
