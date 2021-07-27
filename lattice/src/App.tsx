import React, { useEffect, useState } from 'react';
import CssBaseline from '@material-ui/core/CssBaseline';
import { Route, Switch } from 'react-router-dom';
import { darkTheme, lightTheme } from 'theme/';
import { Home } from 'App/Home';
import { Header } from 'shared/Header';
import { MuiThemeProvider } from '@material-ui/core/styles';
import { Nav } from 'shared/Nav';
import { NotFound } from 'App/NotFound';
import { MoleculaTablesContainer } from 'App/MoleculaTables';
import { QueryContainer } from 'App/Query';
import { QBuilderContainer } from 'App/QBuilder';
import css from './App.module.scss';

const App = () => {
  const [theme, setTheme] = useState<string>(
    localStorage.getItem('theme') || 'light'
  );

  useEffect(() => {
    if(theme === 'dark') {
      document.documentElement.setAttribute('data-theme', 'dark')
    } else {
      document.documentElement.removeAttribute('data-theme');
    }
  }, [theme]);

  const onToggleTheme = () => {
    const newTheme = theme === 'dark' ? 'light' : 'dark';
    setTheme(newTheme);
    localStorage.setItem('theme', newTheme);
  };

  return (
    <MuiThemeProvider theme={theme === 'light' ? lightTheme : darkTheme}>
      <CssBaseline />
      <Header onToggleTheme={onToggleTheme} />

      <div className={css.container}>
        <div className={css.layout}>
          <Nav />

          <div className={css.mainContent}>
            <Switch>
              <Route exact path="/" component={Home} />
              <Route path="/tables/:id?" component={MoleculaTablesContainer} />
              <Route exact path="/query" component={QueryContainer} />
              <Route exact path="/querybuilder" component={QBuilderContainer} />
              <Route component={NotFound} />
            </Switch>
          </div>
        </div>
      </div>
    </MuiThemeProvider>
  );
}

export default App;
