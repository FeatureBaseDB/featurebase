import React from "react";
import { Button } from "@material-ui/core";

interface Props {
  children?: React.ReactNode;
}

const SignOutButton: React.FC<Props> = ({ children }) => {
  const signoutOnClick = (e) => {
    window.location.href = "/logout";
  };

  return (
    <Button variant="contained" color="secondary" onClick={signoutOnClick}>
      Sign out
    </Button>
  );
};

export default SignOutButton;
