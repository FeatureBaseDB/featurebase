import React from 'react';

import { Button } from '@material-ui/core';

interface Props {
  children?: React.ReactNode;
}

const SignOutButton: React.FC<Props> = ({ children }) => {
  return (
    <a href="/logout">
      <Button variant="contained" color="secondary">
        Signout
      </Button>
    </a>
  );
};

export default SignOutButton;
