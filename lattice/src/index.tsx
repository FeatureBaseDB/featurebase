import React from 'react';
import ReactDOM from 'react-dom';
import { ProvideAuth } from 'services/useAuth';

import * as serviceWorker from './serviceWorker';
import './index.scss';
import App from './App';

ReactDOM.render(
  <React.StrictMode>
    <ProvideAuth>
      <App />
    </ProvideAuth>
  </React.StrictMode>,
  document.getElementById('root')
);

// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: https://bit.ly/CRA-PWA
serviceWorker.unregister();
