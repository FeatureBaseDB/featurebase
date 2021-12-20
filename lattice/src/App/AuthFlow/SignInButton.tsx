import React from 'react';
import { Button } from '@material-ui/core';

interface Props {
  children?: React.ReactNode;
}

const SignInButton: React.FC<Props> = ({ children }) => {
  return (
    <a href="/login">
      <Button variant="contained" color="primary" size="large" fullWidth>
        Sign in
      </Button>
    </a>
  );
};

export default SignInButton;
