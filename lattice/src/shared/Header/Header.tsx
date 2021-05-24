import React, { FC } from 'react';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import { Link } from 'react-router-dom';
import { ReactComponent as MoleculaLogo } from 'assets/lightTheme/MoleculaLogo.svg';
import { ReactComponent as MoleculaLogoDark } from 'assets/darkTheme/MoleculaLogo.svg';
import { ThemeToggle } from 'shared/ThemeToggle';
import { useTheme } from '@material-ui/core/styles';
import css from './Header.module.scss';

type HeaderProps = {
  onToggleTheme: () => void;
};

export const Header: FC<HeaderProps> = ({ onToggleTheme }) => {
  const theme = useTheme();
  const isDark = theme.palette.type === 'dark';

  return (
    <AppBar
      className={css.appToolbar}
      position="static"
      color="default"
      elevation={0}
    >
      <Toolbar>
        <div className={css.toolbarLayout}>
          <Link to="/" className={css.homeLink}>
            {!isDark && <MoleculaLogo className={css.logo} />}
            {isDark && <MoleculaLogoDark className={css.logo} />}
          </Link>

          <div className={css.toolbarActions}>
            <div className={css.toggle}>
              <ThemeToggle
                theme={theme.palette.type}
                onToggle={onToggleTheme}
              />
            </div>
          </div>
        </div>
      </Toolbar>
    </AppBar>
  );
};
