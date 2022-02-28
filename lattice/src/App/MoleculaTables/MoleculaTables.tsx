import React, { FC, Fragment, useEffect, useState } from 'react';
import Card from '@material-ui/core/Card';
import CardContent from '@material-ui/core/CardContent';
import moment from 'moment';
import OrderBy from 'lodash/orderBy';
import Paper from '@material-ui/core/Paper';
import Tooltip from '@material-ui/core/Tooltip';
import Typography from '@material-ui/core/Typography';
import { Block } from 'shared/Block';
import { SortBy } from 'shared/SortBy';
import { useHistory } from 'react-router-dom';
import css from './MoleculaTables.module.scss';

type MoleculaTablesProps = {
  tables: any;
  lastUpdated: string;
  maxSize: number;
};

export const MoleculaTables: FC<MoleculaTablesProps> = ({
  tables,
  lastUpdated,
  maxSize,
}) => {
  const history = useHistory();
  const [sortedTables, setSortedTables] = useState<any>([]);
  const lastUpdatedMoment = lastUpdated ? moment(lastUpdated).utc() : undefined;

  useEffect(() => {
    if (tables) {
      setSortedTables(tables);
    }
  }, [tables]);

  const handleSortChange = (value: any) => {
    const sortDirection = value === 'name' ? 'asc' : 'desc';
    setSortedTables(OrderBy(sortedTables, [value], [sortDirection]));
  };

  return (
    <Fragment>
      <Block>
        <Typography variant="h5" color="textSecondary">
          Tables
        </Typography>
        {lastUpdatedMoment ? (
          <div className={css.infoMessage}>
            Disk usage last updated{' '}
            <Tooltip
              title={`${lastUpdatedMoment.format('M/D/YYYY hh:mm a')} UTC`}
              placement="top"
              arrow
            >
              <span className={css.infoTooltip}>
                {lastUpdatedMoment.fromNow()}
              </span>
            </Tooltip>
            . Disk usage for new tables will be calculated at the next{` `}
            <Tooltip
              title={
                <Fragment>
                  Disk and memory information shown here are read from a cache,
                  the behavior of which can be controlled with the{` `}
                  <code style={{ whiteSpace: 'nowrap' }}>
                    --usage-duty-cycle
                  </code>{' '}
                  command line flag.
                </Fragment>
              }
              placement="top"
              arrow
            >
              <span className={css.infoTooltip}>cache refresh</span>
            </Tooltip>
            .
          </div>
        ) : null}
        <div className={css.actions}>
          <SortBy
            options={[
              { label: 'Name', value: 'name' },
              { label: 'Index Size', value: 'total' },
              { label: 'Index Keys Size', value: 'indexKeys' },
              { label: 'Fragment Size', value: 'fragments' },
              { label: 'Field Keys Size', value: 'fieldKeysTotal' },
              { label: 'Metadata Size', value: 'metadata' },
            ]}
            defaultValue="name"
            onChange={handleSortChange}
            sortId="sort-by"
          />
        </div>
        <div className={css.tiles}>
          {sortedTables.map((table) => {
            const { name, options, fields } = table;

            return (
              <Card key={name} className={css.tableTile}>
                <CardContent>
                  <div className={css.header}>{name}</div>
                  <label className={css.label}>Options</label>
                  <div className={css.cell}>
                    <span className={css.label}>keys</span>
                    <code className={css.code}>
                      {options.keys ? 'TRUE' : 'FALSE'}
                    </code>
                  </div>
                  <div className={css.showDetails}>
                    <span
                      className={css.link}
                      onClick={() => history.push(`/tables/${name}`)}
                    >
                      Show Fields ({fields.length})
                    </span>
                  </div>
                </CardContent>
              </Card>
            );
          })}
        </div>
        {tables && tables.length === 0 && (
          <Paper className={css.pilosaError}>
            <Typography variant="caption" color="textSecondary">
              There are no tables to show.
            </Typography>
          </Paper>
        )}
        {!tables && (
          <Paper className={css.pilosaError}>
            <Typography variant="caption" color="textSecondary">
              There is a problem connecting to Pilosa.
            </Typography>
          </Paper>
        )}
      </Block>
    </Fragment>
  );
};
