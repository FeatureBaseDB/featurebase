import React, { FC, Fragment } from 'react';
import classNames from 'classnames';
import Tooltip from '@material-ui/core/Tooltip';
import Typography from '@material-ui/core/Typography';
import { formatBytes } from 'shared/utils/formatBytes';
import css from './UsageBreakdown.module.scss';

type UsageBreakdownProps = {
  data: any;
  width?: string;
  showLabel?: boolean;
  usageValueSize?: 'small' | 'medium';
};

export const UsageBreakdown: FC<UsageBreakdownProps> = ({
  data = {},
  width,
  showLabel = true,
  usageValueSize = 'medium'
}) => {
  const {
    total,
    fieldKeysTotal,
    indexKeys,
    fragments,
    metadata,
    keys,
    uncached
  } = data;
  const fieldKeysPercentage =
    fieldKeysTotal && total ? (fieldKeysTotal / total) * 100 : 0;
  const indexKeysPercentage = indexKeys ? (indexKeys / total) * 100 : 0;
  const fragmentsPercentage = fragments ? (fragments / total) * 100 : 0;
  const metadataPercentage = metadata ? (metadata / total) * 100 : 0;
  const keysPercentage = keys && total ? (keys / total) * 100 : 0;

  return (
    <Fragment>
      {showLabel ? <label className={css.label}>Data</label> : null}
      <div className={css.usageBreakdown}>
        {total ? (
          <Fragment>
            <span
              className={classNames(css.usageBreakdownLabel, {
                [css.smallLabel]: usageValueSize === 'small'
              })}
            >
              {formatBytes(total)}
            </span>
            <div
              className={css.breakdown}
              style={{ width: width ? width : '100%' }}
            >
              {fieldKeysTotal ? (
                <Tooltip
                  title={
                    <Fragment>
                      <label>Field Keys:</label>
                      <Typography variant="caption" component="div">
                        {formatBytes(fieldKeysTotal)} (
                        {fieldKeysPercentage.toLocaleString(undefined, {
                          maximumFractionDigits: 1
                        })}
                        %)
                      </Typography>
                    </Fragment>
                  }
                  placement="top"
                  arrow
                >
                  <div
                    className={classNames(css.bar, css.fieldKeysTotal)}
                    style={{ width: `${fieldKeysPercentage}%` }}
                  />
                </Tooltip>
              ) : null}
              {indexKeys ? (
                <Tooltip
                  title={
                    <Fragment>
                      <label>Index Keys:</label>
                      <Typography variant="caption" component="div">
                        {formatBytes(indexKeys)} (
                        {indexKeysPercentage.toLocaleString(undefined, {
                          maximumFractionDigits: 1
                        })}
                        %)
                      </Typography>
                    </Fragment>
                  }
                  placement="top"
                  arrow
                >
                  <div
                    className={classNames(css.bar, css.indexKeys)}
                    style={{ width: `${indexKeysPercentage}%` }}
                  />
                </Tooltip>
              ) : null}
              {keys ? (
                <Tooltip
                  title={
                    <Fragment>
                      <label>Keys:</label>
                      <Typography variant="caption" component="div">
                        {formatBytes(keys)} (
                        {keysPercentage.toLocaleString(undefined, {
                          maximumFractionDigits: 1
                        })}
                        %)
                      </Typography>
                    </Fragment>
                  }
                  placement="top"
                  arrow
                >
                  <div
                    className={classNames(css.bar, css.keys)}
                    style={{ width: `${keysPercentage}%` }}
                  />
                </Tooltip>
              ) : null}
              {fragments ? (
                <Tooltip
                  title={
                    <Fragment>
                      <label>Fragments:</label>
                      <Typography variant="caption" component="div">
                        {formatBytes(fragments)} (
                        {fragmentsPercentage.toLocaleString(undefined, {
                          maximumFractionDigits: 1
                        })}
                        %)
                      </Typography>
                    </Fragment>
                  }
                  placement="top"
                  arrow
                >
                  <div
                    className={classNames(css.bar, css.fragments)}
                    style={{ width: `${fragmentsPercentage}%` }}
                  />
                </Tooltip>
              ) : null}
              {metadata ? (
                <Tooltip
                  title={
                    <Fragment>
                      <label>Metadata:</label>
                      <Typography variant="caption" component="div">
                        {formatBytes(metadata)} (
                        {metadataPercentage.toLocaleString(undefined, {
                          maximumFractionDigits: 1
                        })}
                        %)
                      </Typography>
                    </Fragment>
                  }
                  placement="top"
                  arrow
                >
                  <div
                    className={classNames(css.bar, css.metadata)}
                    style={{ width: `${metadataPercentage}%` }}
                  />
                </Tooltip>
              ) : null}
            </div>
          </Fragment>
        ) : uncached ? (
          <Typography variant="caption" component="div">
            Waiting...
          </Typography>
        ) : (
          <Typography variant="caption" component="div">
            Calculating...
          </Typography>
        )}
      </div>
    </Fragment>
  );
};
