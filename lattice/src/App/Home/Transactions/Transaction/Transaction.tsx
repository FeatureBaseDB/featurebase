import React, { FC } from 'react';
import Card from '@material-ui/core/Card';
import CardActions from '@material-ui/core/CardActions';
import CardContent from '@material-ui/core/CardContent';
// import ChevronLeftIcon from '@material-ui/icons/ChevronLeft';
import LooksOneSharpIcon from '@material-ui/icons/LooksOneSharp';
import Moment from 'react-moment';
import Tooltip from '@material-ui/core/Tooltip';
import Typography from '@material-ui/core/Typography';
import { formatTimeoutString } from './utils';
import css from './Transaction.module.scss';

type TransactionProps = {
  transaction: any;
  className?: any;
  forceFinish: (id: string) => void;
};

export const Transaction: FC<TransactionProps> = ({
  transaction,
  className,
  forceFinish
}) => {
  const { id, active, exclusive, timeout, deadline, error } = transaction;

  return (
    <Card className={className}>
      <CardContent>
        <div className={css.header}>
          <span className={css.label}>Transaction Id:</span>
          <span className={css.value}>
            <code>{id}</code>
          </span>
          {exclusive && (
            <Tooltip title="Exclusive" placement="top" arrow>
              <LooksOneSharpIcon
                className={css.exclusiveIcon}
                fontSize="inherit"
              />
            </Tooltip>
          )}
        </div>

        <div className={css.details}>
          <div className={css.cell}>
            <label>Status</label>
            <div className={error ? css.error : ''}>
              {error ? 'Error' : active ? 'Active' : 'Waiting'}
            </div>
          </div>
          <div className={css.cell}>
            <label>Timeout</label>
            <div>
              {typeof timeout === 'string'
                ? formatTimeoutString(timeout)
                : `${timeout} secs`}
            </div>
          </div>
          <div className={css.cell}>
            <label>Deadline</label>
            <Tooltip
              title={<Moment date={deadline} format="MMM D, YYYY hh:mm:ss A" />}
              placement="right"
              arrow
            >
              <Moment date={deadline} fromNow />
            </Tooltip>
          </div>
        </div>

        {error && (
          <Typography
            variant="caption"
            color="error"
            className={css.errorMessage}
          >
            {error}
          </Typography>
        )}

        {/* TODO: Once we have blocked-by */}
        {/* {transaction['blocked-by'] && (
          <div className={css.details}>
            <div className={css.cell}>
              <label>Blocked By</label>
              <div>
                {transaction['blocked-by'].map((txId) => (
                  <div className={css.blockedBy}>
                    <ChevronLeftIcon
                      className={css.nestedIcon}
                      fontSize="small"
                    />
                    <code className={css.blockedById}>{txId}</code>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )} */}
      </CardContent>
      <CardActions>
        <Typography
          className={css.finishLink}
          variant="overline"
          color="primary"
          onClick={() => forceFinish(id)}
        >
          Force Finish
        </Typography>
      </CardActions>
    </Card>
  );
};
