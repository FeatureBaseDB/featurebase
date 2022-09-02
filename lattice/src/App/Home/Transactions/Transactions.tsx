import React, { FC, useCallback, useState, useEffect } from 'react';
import CircularProgress from '@material-ui/core/CircularProgress';
import classNames from 'classnames';
import ErrorSharpIcon from '@material-ui/icons/ErrorSharp';
import IconButton from '@material-ui/core/IconButton';
import List from '@material-ui/core/List';
import ListItem from '@material-ui/core/ListItem';
import LooksOneSharpIcon from '@material-ui/icons/LooksOneSharp';
import Paper from '@material-ui/core/Paper';
import PauseCircleFilledIcon from '@material-ui/icons/PauseCircleFilled';
import Pluralize from 'react-pluralize';
import RefreshIcon from '@material-ui/icons/RefreshSharp';
import Tooltip from '@material-ui/core/Tooltip';
import Typography from '@material-ui/core/Typography';
import { Block } from 'shared/Block';
import { Pager } from 'shared/Pager';
import { pilosa } from 'services/eventServices';
import { Transaction } from './Transaction/Transaction';
import { useEffectOnce } from 'react-use';
import css from './Transactions.module.scss';

export const Transactions: FC<{}> = () => {
  const [lastRefresh, setLastRefresh] = useState<number>(0);
  const [transactions, setTransactions] = useState<any>([]);
  const [page, setPage] = useState<number>(1);
  const [selected, setSelected] = useState<any>();
  const sliceStart = (page - 1) * 5;
  const hasTransactions = transactions.length > 0;

  useEffectOnce(() => {
    getTransactions();
  });

  const tick = useCallback(() => {
    if (lastRefresh >= 29) {
      getTransactions();
    } else {
      setLastRefresh(lastRefresh + 1);
    }
  }, [lastRefresh]);

  useEffect(() => {
    let timer = setTimeout(() => tick(), 1000);
    return () => {
      clearTimeout(timer);
    };
  }, [tick]);

  const getTransactions = () => {
    pilosa.get.transactions().then((res) => {
      const transactionList = res.data;
      setTransactions(transactionList);
      if (transactionList.length > 0) {
        setSelected(transactionList[0]);
      } else {
        setSelected(undefined);
      }
      setLastRefresh(0);
    });
  };

  const forceFinish = (id: string) => {
    pilosa.post.finishTransaction(id).then(() => getTransactions());
  };

  const renderListItem = (transaction: any) => {
    const { id, active, exclusive, error } = transaction;

    return (
      <ListItem
        key={id}
        className={classNames(css.transactionItem, {
          [css.inactive]: !active && !error
        })}
        onClick={() => setSelected(transaction)}
        selected={selected?.id === id}
        divider
        button
      >
        {error && (
          <Tooltip title={error} placement="top" arrow>
            <ErrorSharpIcon
              className={css.errorIcon}
              fontSize="inherit"
              color="error"
            />
          </Tooltip>
        )}
        {active && !error && (
          <Tooltip title="Active" placement="top" arrow>
            <CircularProgress className={css.activeIcon} size="0.75rem" />
          </Tooltip>
        )}
        {!active && !error && (
          <Tooltip title="Waiting" placement="top" arrow>
            <PauseCircleFilledIcon
              className={css.waitingIcon}
              fontSize="inherit"
            />
          </Tooltip>
        )}
        <code>{id}</code>
        {exclusive && (
          <Tooltip title="Exclusive" placement="top" arrow>
            <LooksOneSharpIcon
              className={css.exclusiveIcon}
              fontSize="inherit"
            />
          </Tooltip>
        )}
      </ListItem>
    );
  };

  return (
    <Block>
      <div className={css.transactionsHeader}>
        <Typography variant="h5" color="textSecondary">
          Transactions
        </Typography>
        <div className={css.refresh}>
          <Tooltip
            title={
              <div>
                Last refreshed{' '}
                <Pluralize singular="second" count={lastRefresh} /> ago
              </div>
            }
            placement="top"
            arrow
          >
            <IconButton size="small" color="inherit" onClick={getTransactions}>
              <RefreshIcon color="inherit" fontSize="inherit" />
            </IconButton>
          </Tooltip>
        </div>
      </div>
      <div className={css.layout}>
        <div>
          <Paper>
            <List className={css.list}>
              {transactions
                .slice(sliceStart, sliceStart + 5)
                .map((transaction) => renderListItem(transaction))}

              {!hasTransactions && (
                <ListItem>
                  <Typography variant="caption" color="textSecondary">
                    No active transactions.
                  </Typography>
                </ListItem>
              )}
            </List>
          </Paper>
          {hasTransactions && (
            <Pager
              page={page}
              rowsPerPage={5}
              totalItems={transactions.length}
              onChangePage={setPage}
            />
          )}
        </div>
        {selected && (
          <div className={css.itemWrapper}>
            <div className={css.dividerLeft} />
            <div className={css.dividerRight} />
            <Transaction
              className={css.item}
              transaction={selected}
              forceFinish={forceFinish}
            />
          </div>
        )}
      </div>
    </Block>
  );
};
