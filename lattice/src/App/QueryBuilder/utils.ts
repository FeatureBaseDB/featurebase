import { Operator, RowGrouping } from './rowTypes';
import { getIPRange } from 'get-ip-range';

export const stringifyExtract = (query: any) => {
  const { columns, operator, rowCalls } = query;

  const rowData = stringifyRowData(rowCalls, operator);
  if (!rowData.error) {
    const fields = columns.map((field) => `Rows(${field})`);
    const allRows = fields.join(', ');
    return {
      error: false,
      queryString: `Extract(Limit(${rowData.queryString}, limit=1000), ${allRows})`,
      countQuery: `Count(${rowData.queryString})`
    };
  } else {
    return { ...rowData, countQuery: '' };
  }
}

export const stringifyCount = (query: any) => {
  const { operator, rowCalls } = query;

  const rowData = stringifyRowData(rowCalls, operator);
  if (!rowData.error) {
    return { error: false, queryString: `Count(${rowData.queryString})` };
  } else {
    return rowData;
  }
}

export const stringifyRowData = (rowCalls: RowGrouping[], operator?: Operator) => {
  let queryString = '';
  let error = false;
  if (rowCalls.length === 0) {
    queryString = 'All()';
  } else {
    let rowsMap: string[][] = [];
    rowCalls.forEach((group, groupIdx) => {
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
            return { error: true, queryString: error.message };
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

    if (!error) {
      queryString = rowsMap
        .map((group, idx) => {
          let joined = '';
          if (group.length > 1) {
            joined = group.map((r) => r).join(', ');
            const operator = rowCalls[idx].operator;
            if (operator === 'and') {
              joined = `Intersect(${joined})`;
            } else if (operator === 'or') {
              joined = `Union(${joined})`;
            }
          } else {
            joined = group[0];
          }
          return rowCalls[idx].isNot ? `Not(${joined})` : joined;
        })
        .join(', ');

      if (rowCalls.length > 1 && operator) {
        if (operator === 'and') {
          queryString = `Intersect(${queryString})`;
        } else if (operator === 'or') {
          queryString = `Union(${queryString})`;
        }
      }
    }
  }

  return { error, queryString };
}

export const stringifyGroupBy = (query: any) => {
  const { groupByCall, filter, sort } = query;

  let sortString = '';
  let aggregateString = '';
  if (sort?.length > 0) {
    sortString = sort[0].sortValue;
    if (sort[0].sortValue.includes('sum')) {
      aggregateString = `, aggregate=Sum(field=${sort[0].field})`;
    }
    if (sort.length > 1) {
      sortString = `${sortString}, ${sort[1].sortValue}`;
      if (sort[1].sortValue.includes('sum')) {
        aggregateString = `, aggregate=Sum(field=${sort[1].field})`;
      }
    }
    sortString = `, sort="${sortString}"`;
  }
  const filterString = filter ? `, filter=${filter}` : '';
  const queryString = groupByCall.secondary
    ? `GroupBy(Rows(${groupByCall.primary}), Rows(${groupByCall.secondary})${filterString}${sortString}${aggregateString})`
    : `GroupBy(Rows(${groupByCall.primary})${filterString}${sortString}${aggregateString})`;

  return queryString;
}

export const cleanupRows = (rowCalls: RowGrouping[]) => {
  // remove empty groups and row calls
  let isInvalid = false;
  let cleanRowCalls: RowGrouping[] = [];
  let cleanGroups: RowGrouping[] = [];

  if (rowCalls?.length > 0) {
    rowCalls.forEach((group, groupIdx) => {
      cleanGroups.push({ ...group, row: [] });
      group.row.forEach((row) => {
        const { field, value, rowOperator } = row;
        if (field || value) {
          cleanGroups[groupIdx].row.push(row);

          if (!field || !value) {
            isInvalid = true;
          } else if (rowOperator === 'cidr') {
            try {
              getIPRange(value);
            } catch (err) {
              isInvalid = true;
            }
          }
        }
      });
    });

    cleanGroups.forEach((group) => {
      if (group.row.length > 0) {
        cleanRowCalls.push(group);
      }
    });
  }

  return { cleanRowCalls, isInvalid };
};
