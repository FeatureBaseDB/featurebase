import React, { createContext, useContext, useEffect, useState } from 'react';
import { useHistory } from 'react-router-dom';

import { pilosa } from './eventServices';

const authContext = createContext<any>({});

// Provider component that wraps your app and makes auth object ...
// ... available to any child component that calls useAuth().
export function ProvideAuth({ children }) {
  const auth = useProvideAuth();
  return <authContext.Provider value={auth}>{children}</authContext.Provider>;
}

// Hook for child components to get the auth object ...
// ... and re-render when it changes.
export const useAuth = () => {
  return useContext(authContext);
};

export interface IUser {
  userid: string;
  username: string;
}

// Provider hook that creates auth object and handles state
function useProvideAuth() {
  const history = useHistory();
  const [user, setUser] = useState<IUser | undefined>(undefined);
  const [isAuthenticated, setIsAuthenticated] = useState<boolean>(false);
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [authOn, setAuthOn] = useState<boolean>(true);

  const userinfo = () => {
    pilosa.get.userinfo().then((userinfoRes) => {
      if (userinfoRes.data.userid && userinfoRes.data.username) {
        setUser(userinfoRes.data);
      } else {
        setUser(undefined);
      }
    });
  };

  const signin = () => {
    history.push(`/login`);
  };

  const signout = () => {
    history.push("/logout");
  };
  // Subscribe to user on mount
  // Because this sets state in the callback it will cause any ...
  // ... component that utilizes this hook to re-render with the ...
  // ... latest auth object.
  useEffect(() => {
    pilosa.get
      .auth()
      .then((res) => {
        if (res.status === 204) {
          // Authentication is off
          setAuthOn(false);
        } else {
          // Turn on Authentication 
          setAuthOn(true);

          if (res.data === "OK") {
            // User is authenticated
            setIsAuthenticated(true);

            // get userinfo
            userinfo();
          } else {
            // User not authenticated
            setIsAuthenticated(false);
          }
        }
      })
      .finally(() => {
        setIsLoading(false);
      });
  }, []);

  return {
    isAuthenticated,
    isLoading,
    user,
    authOn,
    userinfo,
    signin,
    signout,
  };
}
