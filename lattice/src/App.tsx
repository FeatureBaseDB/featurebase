import { BrowserRouter, Route, Switch } from "react-router-dom";
import { MuiThemeProvider } from "@material-ui/core/styles";

import Main from "Main";
import Signin from "App/AuthFlow/Signin";
import { useAuth } from "services/useAuth";
import PrivateRoute from "shared/PrivateRoute/PrivateRoute";
import { lightTheme } from "theme/";

const App = () => {
  const auth = useAuth();

  return (
    <BrowserRouter>
      {auth.isLoading ? (
        <div></div>
      ) : (
        <MuiThemeProvider theme={lightTheme}>
          {auth.isAuthOn ? (
            // Auth is on, hide the routes with PrivateRoute
            <Switch>
              <Route
                exact
                path="/signin"
                render={(props) => <Signin {...props}></Signin>}
              />
              <PrivateRoute path="/" component={Main} />
            </Switch>
          ) : (
            // Auth is off, all routes are accessible
            <Route path="/" component={Main} />
          )}
        </MuiThemeProvider>
      )}
    </BrowserRouter>
  );
};

export default App;
