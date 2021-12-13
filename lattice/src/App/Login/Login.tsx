import LoginButton from './LoginButton';
import { pilosa } from 'services/eventServices';

function login() {
  pilosa.get.login().then((res) => {
    console.log(`login result:`, res);
  });
}

function Login() {
  return (
    <>
      <h6>Login</h6>
      <LoginButton onClick={login} />
    </>
  );
}

export default Login;
