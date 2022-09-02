import React from 'react';
import css from './ClusterInfo.module.scss';

const info = {
  shardWidth: 1048576
};

export const ClusterInfo = () => {
  const keys = Object.keys(info);
  return (
    <div className={css.keyValues}>
      {keys.map((key) => (
        <div key={key} className={css.cell}>
          <label>{key}</label>
          <div>{info[key]}</div>
        </div>
      ))}
    </div>
  );
};
