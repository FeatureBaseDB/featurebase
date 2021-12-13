import React from 'react';

interface Props {
  onClick: () => void;
}

const LoginButton: React.FC<Props> = ({ onClick }) => {
  return <button onClick={onClick}>Login</button>;
};

export default LoginButton;
