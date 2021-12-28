import { AxiosResponse } from 'axios';
import { act } from 'react-dom/test-utils';
import ReactDOM from 'react-dom';

import { ProvideAuth, useAuth } from 'services/useAuth';
import { pilosa } from './eventServices';
jest.mock('./eventServices');

const AUTHENTICATED = 'Authenticated';
const NOTAUTHED = 'Not Authed';
const AUTHOFF = 'Auth off';

function TestUseAuthComponent() {
  const auth = useAuth();

  if (auth.isAuthOn === true && auth.isAuthenticated === true) {
    return <div>{AUTHENTICATED}</div>;
  } else if (auth.isAuthOn === true && auth.isAuthenticated === false) {
    return <div>{NOTAUTHED}</div>;
  } else {
    return <div>{AUTHOFF}</div>;
  }
}

beforeEach(() => {
  jest.clearAllMocks();
});

test('useAuth - expect authenticated', async () => {
  const mockResponse: AxiosResponse<any> = {
    status: 200,
    data: 'OK',
    statusText: '',
    headers: {},
    config: {},
  };
  const root = document.createElement('root');
  await act(async () => {
    jest.spyOn(pilosa.get, 'auth').mockResolvedValueOnce(mockResponse);
    ReactDOM.render(
      <ProvideAuth>
        <TestUseAuthComponent />
      </ProvideAuth>,
      root
    );
  });
  expect(pilosa.get.auth).toHaveBeenCalledTimes(1);
  expect(root.innerHTML).toContain(AUTHENTICATED);
});

test('test useAuth - expect not authed', async () => {
  const mockResponse: AxiosResponse<any> = {
    status: 200,
    data: '',
    statusText: '',
    headers: {},
    config: {},
  };

  const root = document.createElement('root');
  await act(async () => {
    jest.spyOn(pilosa.get, 'auth').mockResolvedValueOnce(mockResponse);
    ReactDOM.render(
      <ProvideAuth>
        <TestUseAuthComponent />
      </ProvideAuth>,
      root
    );
  });
  expect(pilosa.get.auth).toHaveBeenCalledTimes(1);
  expect(root.innerHTML).toContain(NOTAUTHED);
});

test('test useAuth - expect auth off', async () => {
  const mockResponse: AxiosResponse<any> = {
    status: 204,
    data: '',
    statusText: '',
    headers: {},
    config: {},
  };

  const root = document.createElement('root');
  await act(async () => {
    jest.spyOn(pilosa.get, 'auth').mockResolvedValueOnce(mockResponse);
    ReactDOM.render(
      <ProvideAuth>
        <TestUseAuthComponent />
      </ProvideAuth>,
      root
    );
  });
  expect(pilosa.get.auth).toHaveBeenCalledTimes(1);
  expect(root.innerHTML).toContain(AUTHOFF);
});
