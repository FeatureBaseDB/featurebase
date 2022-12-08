import React, { FC, Fragment, useState } from 'react';
import CloseIcon from '@material-ui/icons/Close';
import copy from 'copy-to-clipboard';
import FileCopySharpIcon from '@material-ui/icons/FileCopySharp';
import IconButton from '@material-ui/core/IconButton';
import Tooltip from '@material-ui/core/Tooltip';
import Typography from '@material-ui/core/Typography';
import { DataTable } from 'shared/DataTable';
import { formatDuration } from 'shared/utils/formatDuration';
import { GroupByChart } from 'App/QueryBuilder/GroupByChart';
import { ResultType } from '../QueryContainer';
import css from './QueryResults.module.scss';

type QueryResultsProps = {
  collapsibleQuery?: boolean;
  results: ResultType;
  isSQL3?: boolean
  onRemoveResult?: () => void;
};

export const QueryResults: FC<QueryResultsProps> = ({
  collapsibleQuery = true,
  results,
  isSQL3,
  onRemoveResult,
}) => {
  const [showQuery, setShowQuery] = useState<boolean>(false);
  const [copyTooltip, setCopyTooltip] = useState<string>('Copy Query');
  const queryString =
    results.type === 'PQL'
      ? `[${results.index}]${results.query}`
      : results.query;

  const headers = results.headers;
  const data = results.rows.map((row) => {

    let rowData = {};
    row.forEach((col, colIdx) => {
      
      if (isSQL3) {
        const header = headers[colIdx];
        if (Array.isArray(col)) {
          let displayString = col.join(", ");
          rowData[header.name] = displayString;
        } else if (headers[colIdx]['base-type'] == "decimal") {
          const scale = headers[colIdx]['type-info']['scale'];
          rowData[header.name] = Number(col) / Math.pow(10, scale);
        } else {
          rowData[header.name] = col;
        }

      } else {
        const header = headers[colIdx];
        if (header.datatype.includes('[]')) {
          const dataTypeVal = `${header.datatype.slice(2)}arrayval`;
          rowData[header.name] = col[dataTypeVal].valsList.join(', ');
        } else if (header.datatype === 'decimal') {
          const decimalVal = col[`${header.datatype}val`];
          if (decimalVal) {
            const { value, scale } = decimalVal;
            rowData[header.name] = value / Math.pow(10, scale);
          } else {
            rowData[header.name] = decimalVal;
          }
        } else {
          rowData[header.name] = col[`${header.datatype}val`];
        }
      }
    });

    return rowData;
  });

  const onCopyQuery = () => {
    copy(queryString);
    setCopyTooltip('Copied!');
    setTimeout(() => {
      setCopyTooltip('Copy Query');
    }, 1500);
  };

  return (
    <Fragment>
      <div className={css.queryMetadata}>
        <div className={css.queryItemHeader}>
          <div className={css.queryHeader}>
            {!collapsibleQuery ? (
              <Typography color="textSecondary" variant="overline">
                Query
              </Typography>
            ) : (
              <span
                onClick={() => setShowQuery(!showQuery)}
                className={css.link}
              >
                {showQuery ? 'Hide' : 'Show'} Query
              </span>
            )}
            <Tooltip title={copyTooltip} placement="top" arrow>
              <IconButton
                className={css.icon}
                size="small"
                color="inherit"
                onClick={onCopyQuery}
              >
                <FileCopySharpIcon fontSize="inherit" />
              </IconButton>
            </Tooltip>
          </div>
          <div className={css.queryHeaderRight}>
            {results.duration ? (
              <Tooltip
                title={
                  <Typography
                    className={css.durationTooltip}
                    color="textSecondary"
                    variant="caption"
                    component="div"
                  >
                    <div className={css.duration}>
                      {formatDuration(results.duration, true)}
                    </div>
                    <div>| query time</div>
                    <div className={css.duration}>
                      {formatDuration(results.roundtrip)}
                    </div>
                    <div>| total roundtrip time</div>
                  </Typography>
                }
                placement="top"
                arrow
              >
                <Typography
                  color="textSecondary"
                  variant="caption"
                  component="div"
                >
                  {formatDuration(results.duration, true)}
                </Typography>
              </Tooltip>
            ) : null}

            {onRemoveResult ? (
              <IconButton
                className={css.removeItemButton}
                edge="end"
                size="small"
                aria-label="Remove result item"
                onClick={onRemoveResult}
              >
                <CloseIcon fontSize="small" />
              </IconButton>
            ) : null}
          </div>
        </div>
        {!collapsibleQuery || showQuery ? (
          <code className={css.queryString}>{queryString}</code>
        ) : null}
      </div>
      {results.operation === 'GroupBy' && results.rows.length <= 50 ? (
        <GroupByChart results={results} />
      ) : (
        <DataTable
          headers={headers}
          data={data}
          autoWidth={true}
          totalResultsCount={results.totalMessageCount}
        />
      )}
    </Fragment>
  );
};
