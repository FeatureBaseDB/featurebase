import React from 'react';
import { Button } from '@material-ui/core';

interface Props {
  children?: React.ReactNode;
}

const SignInButton: React.FC<Props> = ({ children }) => {
  const signinOnClick = (e) => {
    window.location.href = '/login';
  };

  return (
    <Button variant="contained" color="primary" size="large" onClick={signinOnClick} fullWidth>
      Sign in
    </Button>
  );
};

export default SignInButton;
