import React, { FC } from 'react';
import classNames from 'classnames';
import css from './Toggle.module.scss';

type ToggleProps = {
  on: boolean;
  size?: 'small' | 'default';
  onToggle?: () => void;
};

export const Toggle: FC<ToggleProps> = ({ on, size = 'default', onToggle }) => (
  <div
    className={classNames(css.toggle, {
      [css.small]: size === 'small',
      [css.readOnly]: !onToggle
    })}
  >
    <div
      className={classNames(css.indicator, {
        [css.on]: on
      })}
      onClick={() => {
        if (onToggle) {
          onToggle();
        }
      }}
    />
  </div>
);
