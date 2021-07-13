import React, { FC, useState } from 'react';
import moment, { Moment } from 'moment';
import uniqBy from 'lodash/uniqBy';
import { Query } from './Query';
import { pilosa } from 'services/eventServices';
import { useEffectOnce } from 'react-use';
import { grpc } from '@improbable-eng/grpc-web';
import { queryPQL, querySQL } from 'services/grpcServices';
import { ColumnInfo, ColumnResponse, RowResponse } from 'proto/pilosa_pb';

export type ResultType = {
  query: string;
  operation: string;
  type: 'PQL' | 'SQL';
  headers: ColumnInfo.AsObject[];
  rows: ColumnResponse.AsObject[][];
  duration?: number;
  roundtrip: number;
  index?: string;
  error: string;
};

let streamingResults: ResultType = {
  query: '',
  operation: '',
  type: 'SQL',
  headers: [],
  rows: [],
  roundtrip: 0,
  error: ''
};

export const QueryContainer: FC<{}> = () => {
  let startTime: Moment;
  const [indexes, setIndexes] = useState<any>();
  const [results, setResults] = useState<ResultType[]>([]);
  const [errorResult, setErrorResult] = useState<ResultType>();
  const [loading, setLoading] = useState<boolean>(false);

  useEffectOnce(() => {
    pilosa.get.schema().then((res) => {
      setIndexes(res.data.indexes);
    });
  });

  const handleQueryMessages = (message: RowResponse) => {
    const response = message.toObject();
    if (response.headersList.length > 0) {
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
      let recentQueries = JSON.parse(
        localStorage.getItem('recent-queries') || '[]'
      );
      const lastQuery = localStorage.getItem('last-query');
      recentQueries.unshift(lastQuery);
      recentQueries = uniqBy(recentQueries);

      if (recentQueries.length > 10) {
        localStorage.setItem(
          'recent-queries',
          JSON.stringify(recentQueries.slice(0, 9))
        );
      } else {
        localStorage.setItem('recent-queries', JSON.stringify(recentQueries));
      }

      streamingResults.roundtrip = moment
        .duration(moment().diff(startTime))
        .as('milliseconds');
      setErrorResult(undefined);
      setResults([streamingResults, ...results]);
    }
    setLoading(false);
  };

  const onQuery = (query: string, type: 'PQL' | 'SQL', index?: string) => {
    streamingResults = {
      query,
      operation: '',
      type,
      headers: [],
      rows: [],
      index,
      roundtrip: 0,
      error: ''
    };
    startTime = moment();
    if (query) {
      setLoading(true);
      localStorage.setItem(
        'last-query',
        type === 'PQL' ? `[${index}]${query}` : query
      );

      if (type === 'PQL') {
        if (index) {
          queryPQL(index, query, handleQueryMessages, handleQueryEnd);
        } else {
          streamingResults.error = 'missing index';
          setErrorResult(streamingResults);
          setLoading(false);
        }
      } else {
        let queryArr = query.split(' ');
        queryArr.forEach((word, idx) => {
          if (word.includes('-')) {
            let wordArr = word.split('.');
            wordArr.forEach((section, idx) => {
              if(section.includes('-') && !word.includes('`')) {
                wordArr[idx] = `\`${wordArr[idx]}\``;
              }
            })
            queryArr[idx] = wordArr.join('.');
          }
        });
        querySQL(queryArr.join(' '), handleQueryMessages, handleQueryEnd);
      }
    }
  };

  const removeResultItem = (resultIdx: number) => {
    const resultsClone = [...results];
    resultsClone.splice(resultIdx, 1);
    setResults(resultsClone);
  };

  return (
    <Query
      indexList={indexes}
      onQuery={onQuery}
      results={results}
      error={errorResult}
      loading={loading}
      onClear={() => setResults([])}
      onRemoveResult={removeResultItem}
    />
  );
};
