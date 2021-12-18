import { Home } from "App/Home";
import { MoleculaTablesContainer } from "App/MoleculaTables";
import { NotFound } from "App/NotFound";
import { QueryContainer } from "App/Query";
import { QueryBuilderContainer } from "App/QueryBuilder";
import { useEffect, useState } from "react";
import { Route, Switch } from "react-router-dom";
import { Header } from "shared/Header";
import { Nav } from "shared/Nav";
import { darkTheme, lightTheme } from "theme/";

import CssBaseline from "@material-ui/core/CssBaseline";
import { MuiThemeProvider } from "@material-ui/core/styles";

import css from "./App.module.scss";

const Main = () => {
  const [theme, setTheme] = useState<string>(
    localStorage.getItem("theme") || "light"
  );

  useEffect(() => {
    if (theme === "dark") {
      document.documentElement.setAttribute("data-theme", "dark");
    } else {
      document.documentElement.removeAttribute("data-theme");
    }
  }, [theme]);

  const onToggleTheme = () => {
    const newTheme = theme === "dark" ? "light" : "dark";
    setTheme(newTheme);
    localStorage.setItem("theme", newTheme);
  };

  return (
    <div>
      <MuiThemeProvider theme={theme === "light" ? lightTheme : darkTheme}>
        <CssBaseline />
        <Header onToggleTheme={onToggleTheme} />
        <div className={css.container}>
          <div className={css.layout}>
            <Nav />
            <div className={css.mainContent}>
              <Switch>
                <Route exact path="/" component={Home} />
                <Route
                  path="/tables/:id?"
                  component={MoleculaTablesContainer}
                />
                <Route exact path="/query" component={QueryContainer} />
                <Route
                  exact
                  path="/querybuilder"
                  component={QueryBuilderContainer}
                />
                <Route component={NotFound} />
              </Switch>
            </div>
          </div>
        </div>
      </MuiThemeProvider>
    </div>
  );
};

export default Main;
