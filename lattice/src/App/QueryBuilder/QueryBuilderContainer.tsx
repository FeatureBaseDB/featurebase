import React, { Fragment, useState } from 'react';
import Alert from '@material-ui/lab/Alert';
import ArrowForwardIosIcon from '@material-ui/icons/ArrowForwardIos';
import Button from '@material-ui/core/Button';
import classNames from 'classnames';
import CloseIcon from '@material-ui/icons/Close';
import IconButton from '@material-ui/core/IconButton';
import InfoIcon from '@material-ui/icons/Info';
import moment, { Moment } from 'moment';
import Paper from '@material-ui/core/Paper';
import Split from 'react-split';
import Snackbar from '@material-ui/core/Snackbar';
import Tooltip from '@material-ui/core/Tooltip';
import Typography from '@material-ui/core/Typography';
import { Block } from 'shared/Block';
import { formatDuration } from 'shared/utils/formatDuration';
import { grpc } from '@improbable-eng/grpc-web';
import { pilosa } from 'services/eventServices';
import { QueryBuilder } from './QueryBuilder';
import { queryPQL } from 'services/grpcServices';
import { QueryResults } from 'App/Query/QueryResults';
import { ResultType } from 'App/Query/QueryContainer';
import { RowResponse } from 'proto/pilosa_pb';
import { SavedQueries } from 'App/QueryBuilder/SavedQueries';
import { useEffectOnce } from 'react-use';
import css from './QueryBuilderContainer.module.scss';

let streamingResults: ResultType = {
  query: '',
  operation: '',
  type: 'PQL',
  headers: [],
  rows: [],
  roundtrip: 0,
  error: '',
  totalMessageCount: 0,
};

export const QueryBuilderContainer = () => {
  let startTime: Moment;
  let exportRows: any[] = [];
  const colSizes = JSON.parse(
    localStorage.getItem('builderColSizes') || '[25, 75]'
  );
  const [queriesList, setQueriesList] = useState(
    JSON.parse(localStorage.getItem('saved-queries') || '[]')
  );

  const [tables, setTables] = useState<any[]>([]);
  const [showBuilder, setShowBuilder] = useState<boolean>(true);
  const [results, setResults] = useState<ResultType>();
  const [fullCount, setFullCount] = useState<number>();
  const [recordsCount, setRecordsCount] = useState<number>();
  const [errorResult, setErrorResult] = useState<ResultType>();
  const [error, setError] = useState<string>('');
  const [loading, setLoading] = useState<boolean>(false);
  const [savedQuery, setSavedQuery] = useState<number>(-1);

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
        type: 'text/plain;charset=utf-8',
      });
      element.href = URL.createObjectURL(file);
      element.download = `molecula-${results?.index}-${dateTime}.csv`;
      document.body.appendChild(element);
      element.click();
      exportRows = [];
    }
  };

  const onRunQuery = (
    table: string,
    operation: string,
    query: string,
    countQuery?: string
  ) => {
    streamingResults = {
      query,
      operation,
      type: 'PQL',
      headers: [],
      rows: [],
      index: table,
      roundtrip: 0,
      error: '',
      totalMessageCount: 0,
    };
    startTime = moment();
    setLoading(true);

    if (operation !== 'Count') {
      if (countQuery) {
        pilosa.post.query(table, countQuery).then((res) => {
          setFullCount(res.data.results[0]);
        });
      }

      pilosa.post
        .query(table, `Count(All())`)
        .then((res) => setRecordsCount(res.data.results[0]));
    } else {
      setFullCount(undefined);
    }

    queryPQL(table, query, handleQueryMessages, handleQueryEnd);
  };

  const onClear = () => {
    setResults(undefined);
    setFullCount(undefined);
    setErrorResult(undefined);
  };

  const onRemoveQuery = (queryIdx: number) => {
    let queries = [...queriesList];
    queries.splice(queryIdx, 1);
    localStorage.setItem('saved-queries', JSON.stringify(queries));
    setQueriesList(queries);
  };

  const onSaveQuery = () => {
    const updatedList = JSON.parse(
      localStorage.getItem('saved-queries') || '[]'
    );
    if (savedQuery < 0) {
      setSavedQuery(updatedList.length - 1);
    }
    setQueriesList(updatedList);
  };

  const onExportLogs = () => {
    if (results) {
      const columns = results.rows.map((row) => row[0].uint64val);
      const table = results.index ? results.index : '';
      onExternalLookup(table, columns);
    }
  };

  const onExternalLookup = (table: string, columns: number[]) => {
    const cols = JSON.stringify(columns);
    const query = `ExternalLookup(ConstRow(columns=${cols}), query='select id, "rawlog" from "${table}" where id = ANY($1)')`;
    queryPQL(table, query, handleExternalLookup, handleExternalLookupEnd);
  };

  return (
    <Fragment>
      {tables.length > 0 ? (
        <Split
          sizes={showBuilder ? colSizes : [0, 100]}
          cursor="col-resize"
          minSize={showBuilder ? 350 : 50}
          onDragEnd={(sizes) =>
            localStorage.setItem('builderColSizes', JSON.stringify(sizes))
          }
          gutter={(_index, direction) => {
            const gutter = document.createElement('div');
            gutter.className = `gutter gutter-${direction}`;
            const dragbars = document.createElement('div');
            dragbars.className = 'dragBar';
            gutter.appendChild(dragbars);
            return gutter;
          }}
          className={classNames(css.split, !showBuilder ? 'hide-gutter' : '')}
        >
          {showBuilder ? (
            <div className={css.builderColumn}>
              <div className={css.openClose}>
                <IconButton onClick={() => setShowBuilder(false)} size="small">
                  <CloseIcon />
                </IconButton>
              </div>
              <QueryBuilder
                tables={tables}
                savedQuery={savedQuery}
                onRun={onRunQuery}
                onClear={onClear}
                onExitEdit={() => setSavedQuery(-1)}
                onSaveQuery={onSaveQuery}
              />
            </div>
          ) : (
            <div className={css.collapsedBuilder}>
              <IconButton onClick={() => setShowBuilder(true)} size="small">
                <ArrowForwardIosIcon />
              </IconButton>
            </div>
          )}
          <div className={css.results}>
            <Block className={css.resultsBlock}>
              <div className={css.resultsHeader}>
                <Typography variant="h5" color="textSecondary">
                  Results{' '}
                </Typography>
                {results?.query.includes('Extract(') ? (
                  <div className={css.download}>
                    <Tooltip
                      className={css.downloadInfo}
                      title="Download raw data from query results"
                      placement="top"
                      arrow
                    >
                      <InfoIcon fontSize="inherit" />
                    </Tooltip>
                    <Button onClick={onExportLogs}>Download</Button>
                  </div>
                ) : null}
              </div>
              {loading ? <div>Loading...</div> : null}
              {results && !loading ? (
                <Fragment>
                  {fullCount && recordsCount ? (
                    <div className={css.infoMessage}>
                      {results.duration ? (
                        <div>
                          {recordsCount.toLocaleString()} records scanned in{' '}
                          {formatDuration(results.duration, true)}.
                        </div>
                      ) : null}
                      <div>
                        Showing{' '}
                        {fullCount > 1000 ? 'first 1,000 rows of' : 'all'}{' '}
                        {fullCount.toLocaleString()} results.
                      </div>
                    </div>
                  ) : null}
                  <QueryResults results={results} />
                  <div className={css.spacer} />
                </Fragment>
              ) : null}
              {errorResult && !loading ? <div>{errorResult.error}</div> : null}
              {!loading && !results && !error ? (
                <Fragment>
                  <Typography variant="caption" color="textSecondary">
                    Build a query to see results
                  </Typography>
                  {queriesList.length > 0 ? (
                    <div className={css.savedQueries}>
                      <Typography variant="h5" color="textSecondary">
                        Saved Queries
                      </Typography>
                      <SavedQueries
                        queries={queriesList}
                        tables={tables}
                        onClickSaved={(queryIdx) => setSavedQuery(queryIdx)}
                        onRemoveQuery={onRemoveQuery}
                      />
                    </div>
                  ) : null}
                </Fragment>
              ) : null}
            </Block>
          </div>
          <Snackbar open={!!error}>
            <Alert severity="info" onClose={() => setError('')}>
              {error}
            </Alert>
          </Snackbar>
        </Split>
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
      )}
    </Fragment>
  );
};
