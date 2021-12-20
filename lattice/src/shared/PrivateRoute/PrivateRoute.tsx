import { Redirect, Route } from 'react-router-dom';
import { useAuth } from 'services/useAuth';

function PrivateRoute({ component: Component, ...rest }) {
  const auth = useAuth();

  return (
    <Route
      {...rest}
      render={(props) => {
        // If the user is authed render the component
        if (auth.isAuthenticated) {
          // if (true) {
          return <Component {...rest} {...props} />;
        } else {
          // If they are not then we need to redirect to a public page
          return (
            <Redirect
              to={{
                pathname: "/signin",
                state: {
                  from: props.location,
                },
              }}
            />
          );
        }
      }}
    />
  );
}

export default PrivateRoute;
