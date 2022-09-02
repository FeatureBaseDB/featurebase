import React, { FC } from 'react';
import classNames from 'classnames';
import css from './Block.module.scss';

type BlockProps = {
  children: any;
  className?: any;
  top?: boolean;
  right?: boolean;
  bottom?: boolean;
  left?: boolean;
};

export const Block: FC<BlockProps> = ({
  children,
  className,
  top = false,
  right = false,
  bottom = false,
  left = false
}) => (
  <div
    className={classNames(css.block, className, {
      [css.top]: top,
      [css.right]: right,
      [css.bottom]: bottom,
      [css.left]: left
    })}
  >
    {children && children}
  </div>
);
