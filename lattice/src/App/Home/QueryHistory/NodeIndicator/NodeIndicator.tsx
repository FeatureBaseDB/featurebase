import React, { FC } from 'react';
import classNames from 'classnames';
import Tooltip from '@material-ui/core/Tooltip';
import Typography from '@material-ui/core/Typography';
import { nodeRGBColors } from '../nodeColors';
import css from './NodeIndicator.module.scss';

type NodeIndicatorProps = {
  node: string;
  nodeIdx: number;
  tooltip?: boolean;
  off?: boolean;
  onClick?: () => void;
};

export const NodeIndicator: FC<NodeIndicatorProps> = ({
  node,
  nodeIdx,
  tooltip = false,
  off = false,
  onClick = undefined
}) => {
  if (tooltip) {
    return (
      <Tooltip
        title={
          <Typography variant="caption" component="div">
            {node}
          </Typography>
        }
        placement="top"
        arrow
      >
        <div className={css.wrapper}>
          <div
            className={classNames(css.indicator, { [css.clickable]: onClick })}
            style={{
              backgroundColor: `rgb(${nodeRGBColors[nodeIdx]})`,
              boxShadow: `inset -2px -2px 3px rgba(var(--contrast-rgb), 0.1), 0 0 4px 4px rgba(${nodeRGBColors[nodeIdx]}, 0.1)`,
              opacity: off ? 0.3 : 1
            }}
            onClick={onClick}
          ></div>
        </div>
      </Tooltip>
    );
  } else {
    return (
      <div className={css.wrapper}>
        <div
          className={classNames(css.indicator, { [css.clickable]: onClick })}
          style={{
            backgroundColor: `rgb(${nodeRGBColors[nodeIdx]})`,
            boxShadow: `inset -2px -2px 3px rgba(var(--contrast-rgb), 0.1), 0 0 4px 4px rgba(${nodeRGBColors[nodeIdx]}, 0.1)`,
            opacity: off ? 0.3 : 1
          }}
          onClick={onClick}
        ></div>
      </div>
    );
  }
};
