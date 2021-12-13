import React from 'react';
import Divider from '@material-ui/core/Divider';
import List from '@material-ui/core/List';
import ListItem from '@material-ui/core/ListItem';
import ListSubheader from '@material-ui/core/ListSubheader';
import Typography from '@material-ui/core/Typography';
import { NavLink } from 'react-router-dom';
import { useTheme } from '@material-ui/core/styles';
import css from './Nav.module.scss';

export const Nav = () => {
  const theme = useTheme();
  const isDark = theme.palette.type === 'dark';
  const textColor = isDark ? 'textPrimary' : 'textSecondary';

  return (
    <div className={css.leftNav}>
      <List className={css.navList}>
        <NavLink to="/" exact activeClassName={css.activeNavLink}>
          <ListItem className={css.navLink} button>
            <Typography variant="subtitle2" color={textColor}>
              Home
            </Typography>
          </ListItem>
        </NavLink>
        <ListItem />
        <ListSubheader disableGutters disableSticky>
          <Typography variant="overline" className={css.navHeading}>
            Data
          </Typography>
          <Divider className={css.divider} />
        </ListSubheader>
        <NavLink to="/tables" activeClassName={css.activeNavLink}>
          <ListItem className={css.navLink} button>
            <Typography variant="subtitle2" color={textColor}>
              Tables
            </Typography>
          </ListItem>
        </NavLink>
        <NavLink to="/query" activeClassName={css.activeNavLink}>
          <ListItem className={css.navLink} button>
            <Typography variant="subtitle2" color={textColor}>
              Query
            </Typography>
          </ListItem>
        </NavLink>
        <NavLink to="/querybuilder" activeClassName={css.activeNavLink}>
          <ListItem className={css.navLink} button>
            <Typography variant="subtitle2" color={textColor}>
              Query Builder
            </Typography>
          </ListItem>
        </NavLink>
        <NavLink to="/login" activeClassName={css.activeNavLink}>
          <ListItem className={css.navLink} button>
            <Typography variant="subtitle2" color={textColor}>
              Login
            </Typography>
          </ListItem>
        </NavLink>
      </List>
    </div>
  );
};
