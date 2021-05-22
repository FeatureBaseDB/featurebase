import React, { FC, Fragment, useState } from 'react';
import copy from 'copy-to-clipboard';
import FileCopySharpIcon from '@material-ui/icons/FileCopySharp';
import IconButton from '@material-ui/core/IconButton';
import Tooltip from '@material-ui/core/Tooltip';
import Typography from '@material-ui/core/Typography';
import { MotionGroup, MotionSlideItem } from 'shared/Animations';
import css from './RecentQueries.module.scss';

type RecentQueriesType = {
  onQueryClick?: (query: string) => void;
};

export const RecentQueries: FC<RecentQueriesType> = ({ onQueryClick }) => {
  const [copyText, setCopyText] = useState<string>('Click to copy query');
  const queries = JSON.parse(localStorage.getItem('recent-queries') || '[]');

  const onCopyQuery = (query: string) => {
    copy(query);
    setCopyText('Copied!');
    setTimeout(() => {
      setCopyText('Click to copy query');
    }, 1500);
  };

  const renderItem = (query: string) => (
    <div
      className={css.queryItem}
      onClick={() => (onQueryClick ? onQueryClick(query) : null)}
    >
      <Tooltip title={copyText} placement="top" arrow>
        <IconButton
          className={css.copyIcon}
          size="small"
          color="inherit"
          onClick={(e) => {
            e.stopPropagation();
            onCopyQuery(query);
          }}
        >
          <FileCopySharpIcon fontSize="inherit" />
        </IconButton>
      </Tooltip>
      <Tooltip title="Re-run query" placement="right-end" arrow>
        <Typography
          className={css.queryText}
          color="textSecondary"
          variant="caption"
          component="div"
        >
          {query}
        </Typography>
      </Tooltip>
    </div>
  );

  return (
    <Fragment>
      {queries && queries.length > 0 ? (
        <MotionGroup>
          {queries.map((query) => (
            <MotionSlideItem key={query}>{renderItem(query)}</MotionSlideItem>
          ))}
        </MotionGroup>
      ) : (
        <Typography variant="caption" color="textSecondary" component="div">
          No recent queries.
        </Typography>
      )}
    </Fragment>
  );
};
