import { Operator, RowGrouping } from './rowTypes';
import { getIPRange } from 'get-ip-range';

export const stringifyRowData = (rowData: RowGrouping[], operator?: Operator) => {
  let query = '';
  let rowsMap: string[][] = [];
  rowData.forEach((group, groupIdx) => {
    rowsMap.push([]);
    group.row.forEach((row) => {
      let rowString = '';
      const { field, rowOperator, value, type, keys } = row;
      const isNegatory = rowOperator === '!=';
      const isUnion =
        ['=', '!='].includes(rowOperator) && value.split(',').length > 1;
      const operator = isNegatory ? '=' : rowOperator;
      if (isUnion) {
        const values = value.split(',');
        const unionRows = values
          .map((v) => keys ? `Row(${field}="${v.trim()}")` : `Row(${field}=${v.trim()})`)
          .join(', ');
        rowString = `Union(${unionRows})`;
      } else if (rowOperator === 'cidr') {
        try {
          const ipRange = getIPRange(value);
          const ipRows = ipRange
            .map((ip) => `Row(${field}="${ip}")`)
            .join(', ');
          rowString = `Union(${ipRows})`;
        } catch (error) {
          return { error: true, query: error.message };
        }
      } else if (rowOperator === 'like') {
        if (value.includes('%') || value.includes('_')) {
          rowString = `UnionRows(Rows(field=${field}, like="${value}"))`;
        } else {
          rowString = `UnionRows(Rows(field=${field}, like="%${value}%"))`;
        }
      } else {
        rowString = keys || type === 'timestamp'
          ? `Row(${field}${operator}"${value}")`
          : `Row(${field}${operator}${value})`;
      }

      if (isNegatory) {
        rowsMap[groupIdx].push(`Not(${rowString})`);
      } else {
        rowsMap[groupIdx].push(rowString);
      }
    });
  });

  query = rowsMap
    .map((group, idx) => {
      let joined = '';
      if (group.length > 1) {
        joined = group.map((r) => r).join(', ');
        const operator = rowData[idx].operator;
        if (operator === 'and') {
          joined = `Intersect(${joined})`;
        } else if (operator === 'or') {
          joined = `Union(${joined})`;
        }
      } else {
        joined = group[0];
      }
      return rowData[idx].isNot ? `Not(${joined})` : joined;
    })
    .join(', ');

  if (rowData.length > 1 && operator) {
    if (operator === 'and') {
      query = `Intersect(${query})`;
    } else if (operator === 'or') {
      query = `Union(${query})`;
    }
  }

  return { error: false, query };
}
