import React, { FC } from 'react';
import Brightness3Icon from '@material-ui/icons/Brightness3';
import WbSunnyIcon from '@material-ui/icons/WbSunny';
import classNames from 'classnames';
import css from './ThemeToggle.module.scss';

type ThemeToggleProps = {
  theme: string;
  onToggle: () => void;
};

export const ThemeToggle: FC<ThemeToggleProps> = ({ theme, onToggle }) => (
  <div className={css.toggle} onClick={onToggle}>
    <div className={css.icons}>
      <WbSunnyIcon fontSize="small" />
      <Brightness3Icon fontSize="small" />
    </div>
    <div
      className={classNames(css.indicator, { [css.isDark]: theme === 'dark' })}
    ></div>
  </div>
);
