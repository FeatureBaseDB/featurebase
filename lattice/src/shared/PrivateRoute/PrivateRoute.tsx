import { Redirect, Route } from 'react-router-dom';
import { useAuth } from 'services/useAuth';

function PrivateRoute({ component: Component, ...rest }) {
  const auth = useAuth();

  return (
    <Route
      {...rest}
      render={(props) => {
        if (auth.isAuthenticated) {
          // If the user is authenticated, render the component
          return <Component {...rest} {...props} />;
        } else {
          // If the user is not authenticated, redirect to sign in page
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
