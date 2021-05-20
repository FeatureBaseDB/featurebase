import React, { FC, Fragment } from 'react';
import { ClusterHealth } from 'App/Home/ClusterHealth';
import { QueryHistory } from 'App/Home/QueryHistory';
import { Transactions } from 'App/Home/Transactions';
import css from './Home.module.scss';

export const Home: FC = () => (
  <Fragment>
    <ClusterHealth />
    <Transactions />
    <QueryHistory />
    <div className={css.prodPadSpacer} />
  </Fragment>
);
