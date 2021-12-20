import Login from 'App/AuthFlow/Login';
import Main from 'Main';
import { BrowserRouter, Route, Switch } from 'react-router-dom';
import { useAuth } from 'services/useAuth';
import PrivateRoute from 'shared/PrivateRoute/PrivateRoute';
import { lightTheme } from 'theme/';

import { MuiThemeProvider } from '@material-ui/core/styles';

const App = () => {
  const auth = useAuth();

  return (
    <BrowserRouter>
      {auth.isLoading ? (
        <div></div>
      ) : (
        <MuiThemeProvider theme={lightTheme}>
          {auth.isAuthOn ? (
            <Switch>
              <Route
                exact
                path="/signin"
                render={(props) => <Login {...props} name="Login"></Login>}
              />
              <PrivateRoute path="/" component={Main} />
            </Switch>
          ) : (
            <Route path="/" component={Main} />
          )}
        </MuiThemeProvider>
      )}
    </BrowserRouter>
  );
};

export default App;
