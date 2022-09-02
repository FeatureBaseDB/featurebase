import React, { FC, Fragment } from 'react';
import classNames from 'classnames';
import Tooltip from '@material-ui/core/Tooltip';
import Typography from '@material-ui/core/Typography';
import css from './StatusIndicator.module.scss';

type StatusIndicatorProps = {
  state: string;
  stateDetails: { label: string; status: string };
  size?: 'small' | 'medium' | 'large';
  margin?: 'left' | 'right' | 'both';
};

export const StatusIndicator: FC<StatusIndicatorProps> = ({
  state,
  stateDetails,
  size = 'medium',
  margin
}) => {
  const { label, status } = stateDetails;

  return (
    <Tooltip
      title={
        <Fragment>
          <Typography variant="overline">{state}</Typography>
          {label ? (
            <Typography variant="caption" component="div">
              {label}
            </Typography>
          ) : null}
        </Fragment>
      }
      placement="bottom"
      arrow
    >
      <div className={classNames(css.wrapper, margin ? css[margin] : '')}>
        <div className={classNames(css.indicator, css[status], css[size])} />
      </div>
    </Tooltip>
  );
};
