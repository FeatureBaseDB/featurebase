import React, { Fragment, useState } from 'react';
import moment, { Moment } from 'moment';
import Alert from '@material-ui/lab/Alert';
import Paper from '@material-ui/core/Paper';
import Snackbar from '@material-ui/core/Snackbar';
import Typography from '@material-ui/core/Typography';
import { Block } from 'shared/Block';
import { Operator, RowGrouping, RowsCallType } from './rowTypes';
import { pilosa } from 'services/eventServices';
import { QueryBuilder } from './QueryBuilder';
import { useEffectOnce } from 'react-use';
import { ResultType } from 'App/Query/QueryContainer';
import { queryPQL } from 'services/grpcServices';
import { grpc } from '@improbable-eng/grpc-web';
import { RowResponse } from 'proto/pilosa_pb';
import { getIPRange } from 'get-ip-range';
import css from './QueryBuilderContainer.module.scss';

let streamingResults: ResultType = {
  query: '',
  operation: '',
  type: 'PQL',
  headers: [],
  rows: [],
  roundtrip: 0,
  error: ''
};

export const QueryBuilderContainer = () => {
  let startTime: Moment;
  let exportRows: any[] = [];
  const [tables, setTables] = useState<any[]>();
  const [results, setResults] = useState<ResultType>();
  const [fullCount, setFullCount] = useState<number>();
  const [recordsCount, setRecordsCount] = useState<number>();
  const [errorResult, setErrorResult] = useState<ResultType>();
  const [error, setError] = useState<string>('');
  const [loading, setLoading] = useState<boolean>(false);

  useEffectOnce(() => {
    pilosa.get.schema().then((res) => {
      setTables(res.data.indexes);
    });
  });

  const handleQueryMessages = (message: RowResponse) => {
    const response = message.toObject();
    if (response.headersList.length > 0 && response.duration > 0) {
      streamingResults.headers = response.headersList;
      streamingResults.duration = response.duration;
    }
    streamingResults.rows.push(response.columnsList);
  };

  const handleQueryEnd = (status: grpc.Code, statusMessage: string) => {
    if (status !== grpc.Code.OK) {
      streamingResults.error = statusMessage;
      setErrorResult(streamingResults);
    } else {
      streamingResults.roundtrip = moment
        .duration(moment().diff(startTime))
        .as('milliseconds');
      setErrorResult(undefined);
      setResults(streamingResults);
    }
    setLoading(false);
  };

  const onRunQuery = (
    table: any,
    operation: string,
    rowData: RowGrouping[],
    columnsList: string[],
    operator?: Operator
  ) => {
    streamingResults = {
      query: '',
      operation,
      type: 'PQL',
      headers: [],
      rows: [],
      index: table.name,
      roundtrip: 0,
      error: ''
    };
    startTime = moment();
    setLoading(true);
    let query: string = '';
    if (rowData.length === 0) {
      query = 'All()';
    } else {
      let rowsMap: string[][] = [];
      rowData.forEach((group, groupIdx) => {
        rowsMap.push([]);
        group.row.forEach((row) => {
          let rowString = '';
          const { field, rowOperator, value, type } = row;
          const isNegatory = rowOperator === '!=';
          const isUnion =
            ['=', '!='].includes(rowOperator) && value.split(',').length > 1;
          const operator = isNegatory ? '=' : rowOperator;
          if (isUnion) {
            const values = value.split(',');
            const unionRows = values
              .map((v) => `Row(${field}="${v.trim()}")`)
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
              streamingResults.error = error.message;
            }
          } else {
            rowString = ['set', 'timestamp'].includes(type)
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
    }

    if (operation !== 'Count') {
      const countQuery = `Count(${query})`;
      pilosa.post
        .query(table.name, countQuery)
        .then((res) => setFullCount(res.data.results[0]));

      pilosa.post
        .query(table.name, `Count(All())`)
        .then((res) => setRecordsCount(res.data.results[0]));
    } else {
      setFullCount(undefined);
    }

    if (operation === 'Extract') {
      const fields = columnsList.map((field) => `Rows(${field})`);
      const allRows = fields.join(', ');
      query = `${operation}(Limit(${query}, limit=1000), ${allRows})`;
    } else {
      query = `${operation}(${query})`;
    }

    streamingResults.query = query;
    queryPQL(table.name, query, handleQueryMessages, handleQueryEnd);
  };

  const handleExternalLookup = (message: RowResponse) => {
    const response = message.toObject();
    let rowStr: string[] = [];
    if (exportRows.length === 0) {
      const headers = response.headersList.map((header) => header.name);
      exportRows.push(headers.join('\t'));
    }
    response.headersList.forEach((header, idx) =>
      rowStr.push(response.columnsList[idx][`${header.datatype}val`])
    );
    exportRows.push(rowStr.join('\t'));
  };

  const handleExternalLookupEnd = (
    status: grpc.Code,
    statusMessage: string
  ) => {
    if (status !== grpc.Code.OK) {
      setError(statusMessage);
    } else if (exportRows.length === 0) {
      setError('No record attributes for current query.');
    } else {
      const dateTime = moment().unix();
      const element = document.createElement('a');
      const file = new Blob([exportRows.join('\n')], {
        type: 'text/plain;charset=utf-8'
      });
      element.href = URL.createObjectURL(file);
      element.download = `molecula-${results?.index}-${dateTime}.csv`;
      document.body.appendChild(element);
      element.click();
      exportRows = [];
    }
  };

  const onRunGroupBy = (table: any, rowsData: RowsCallType) => {
    streamingResults = {
      query: '',
      operation: 'GroupBy',
      type: 'PQL',
      headers: [],
      rows: [],
      index: table.name,
      roundtrip: 0,
      error: ''
    };
    startTime = moment();
    setLoading(true);
    const query = rowsData.secondary
      ? `GroupBy(Rows(${rowsData.primary}), Rows(${rowsData.secondary}))`
      : `GroupBy(Rows(${rowsData.primary}))`;
    streamingResults.query = query;
    queryPQL(table.name, query, handleQueryMessages, handleQueryEnd);
  };

  const onExternalLookup = (table: string, columns: number[]) => {
    const cols = JSON.stringify(columns);
    const query = `ExternalLookup(ConstRow(columns=${cols}), query='select id, "rawlog" from "${table}" where id = ANY($1)')`;
    queryPQL(table, query, handleExternalLookup, handleExternalLookupEnd);
  };

  return tables && tables.length > 0 ? (
    <Fragment>
      <QueryBuilder
        tables={tables}
        results={results}
        fullResultsCount={fullCount}
        fullRecordsCount={recordsCount}
        error={errorResult}
        loading={loading}
        onQuery={onRunQuery}
        onRunGroupBy={onRunGroupBy}
        onExternalLookup={onExternalLookup}
        onClear={() => {
          setResults(undefined);
          setFullCount(undefined);
          setErrorResult(undefined);
        }}
      />
      <Snackbar open={!!error}>
        <Alert severity="info" onClose={() => setError('')}>
          {error}
        </Alert>
      </Snackbar>
    </Fragment>
  ) : (
    <Block>
      <Typography variant="h5" color="textSecondary">
        Query Builder
      </Typography>
      <Paper className={css.noTables}>
        <Typography variant="caption" color="textSecondary">
          There are no tables to query.
        </Typography>
      </Paper>
    </Block>
  );
};
