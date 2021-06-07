import { stringifyRowData } from './stringifyRowData';

describe('int types', () => {
  it('stringifies = operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "=",
        "value": "0",
        "type": "int"
      }]
    }])).toEqual({ error: false, query: 'Row(fieldName=0)' })
  });

  it('stringifies != operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "!=",
        "value": "0",
        "type": "int"
      }]
    }])).toEqual({ error: false, query: 'Not(Row(fieldName=0))' })
  });

  it('stringifies > operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": ">",
        "value": "0",
        "type": "int"
      }]
    }])).toEqual({ error: false, query: 'Row(fieldName>0)' })
  });

  it('stringifies >= operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": ">=",
        "value": "0",
        "type": "int"
      }]
    }])).toEqual({ error: false, query: 'Row(fieldName>=0)' })
  });

  it('stringifies negated operatorations', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": ">=",
        "value": "0",
        "type": "int"
      }],
      isNot: true
    }])).toEqual({ error: false, query: 'Not(Row(fieldName>=0))' })
  });
})

describe('set types', () => {
  it('stringifies is operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "=",
        "value": "value",
        "type": "set"
      }]
    }])).toEqual({ error: false, query: 'Row(fieldName="value")' })
  });

  it('stringifies is operator for multiple values', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "=",
        "value": "one, two",
        "type": "set"
      }]
    }])).toEqual({ error: false, query: 'Union(Row(fieldName="one"), Row(fieldName="two"))' })
  });

  it('stringifies is not operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "!=",
        "value": "value",
        "type": "set"
      }]
    }])).toEqual({ error: false, query: 'Not(Row(fieldName="value"))' })
  });

  it('stringifies is not operator for multiple values', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "!=",
        "value": "one, two",
        "type": "set"
      }]
    }])).toEqual({ error: false, query: 'Not(Union(Row(fieldName="one"), Row(fieldName="two")))' })
  });

  it('stringifies like operator with a wildcard', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "like",
        "value": "value%",
        "type": "set"
      }]
    }])).toEqual({ error: false, query: 'UnionRows(Rows(field=fieldName, like="value%"))' })
  });

  it('stringifies like operator without a wildcard', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "like",
        "value": "value",
        "type": "set"
      }]
    }])).toEqual({ error: false, query: 'UnionRows(Rows(field=fieldName, like="%value%"))' })
  });

  it('stringifies CIDR operator', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "cidr",
        "value": "10.164.124.33/30",
        "type": "set"
      }]
    }])).toEqual({ error: false, query: 'Union(Row(fieldName="10.164.124.32"), Row(fieldName="10.164.124.33"), Row(fieldName="10.164.124.34"), Row(fieldName="10.164.124.35"))' })
  });

  it('stringifies negated operatorations', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "fieldName",
        "rowOperator": "like",
        "value": "value",
        "type": "set"
      }],
      isNot: true
    }])).toEqual({ error: false, query: 'Not(UnionRows(Rows(field=fieldName, like="%value%")))' })
  });
});

describe('mixed types', () => {
  it('stringifies multiple and-ed rows', () => {
    expect(stringifyRowData([{
      row: [{
        "field": "stringType",
        "rowOperator": "like",
        "value": "value",
        "type": "set"
      }, {
        "field": "intType",
        "rowOperator": "=",
        "value": "0",
        "type": "int"
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
        "type": "set"
      }, {
        "field": "intType",
        "rowOperator": "=",
        "value": "0",
        "type": "int"
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
        "type": "set"
      }, {
        "field": "intType",
        "rowOperator": "=",
        "value": "0",
        "type": "int"
      }],
      operator: "or",
      isNot: true
    }])).toEqual({ error: false, query: 'Not(Union(UnionRows(Rows(field=stringType, like="%value%")), Row(intType=0)))' })
  });
})
