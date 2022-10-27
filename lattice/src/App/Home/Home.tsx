import React, { FC, Fragment } from 'react';
import { ClusterHealth } from 'App/Home/ClusterHealth';
import { QueryHistory } from 'App/Home/QueryHistory';
import { Transactions } from 'App/Home/Transactions';
import css from './Home.module.scss';
import { Link, Typography } from '@material-ui/core';

export const Home: FC = () => (
  <Fragment>
      <Typography variant="h6" color="textPrimary" align="center" style={{padding: 30, paddingBottom: 0}}>
        Try FeatureBase Cloud at
        <span> </span>
        <Link href="https://cloud.featurebase.com/" underline="always">
          cloud.featurebase.com
        </Link>
      </Typography>
    <ClusterHealth/>
    <Transactions/>
    <QueryHistory/>
    <div className={css.prodPadSpacer} />
  </Fragment>
);
