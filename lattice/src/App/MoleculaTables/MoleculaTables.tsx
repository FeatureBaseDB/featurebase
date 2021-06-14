import React, { FC, Fragment, useEffect, useState } from 'react';
import Card from '@material-ui/core/Card';
import CardContent from '@material-ui/core/CardContent';
import OrderBy from 'lodash/orderBy';
import Paper from '@material-ui/core/Paper';
import Typography from '@material-ui/core/Typography';
import { Block } from 'shared/Block';
import { SortBy } from 'shared/SortBy';
import { UsageBreakdown } from './UsageBreakdown';
import { useHistory } from 'react-router-dom';
import css from './MoleculaTables.module.scss';

type MoleculaTablesProps = {
  tables: any;
  dataDistribution: any;
  maxSize: number;
};

export const MoleculaTables: FC<MoleculaTablesProps> = ({
  tables,
  dataDistribution,
  maxSize
}) => {
  const history = useHistory();
  const [sortedTables, setSortedTables] = useState<any>([]);

  useEffect(() => {
    if (tables && dataDistribution) {
      let aggregatedData: any[] = [];
      tables.forEach((i) =>
        aggregatedData.push({
          ...dataDistribution[i.name],
          ...i
        })
      );

      setSortedTables(aggregatedData);
    } else if (tables) {
      setSortedTables(tables);
    }
  }, [tables, dataDistribution]);

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
        <div className={css.actions}>
          <SortBy
            options={[
              { label: 'Name', value: 'name' },
              { label: 'Index Size', value: 'total' },
              { label: 'Index Keys Size', value: 'indexKeys' },
              { label: 'Fragment Size', value: 'fragments' },
              { label: 'Field Keys Size', value: 'fieldKeysTotal' },
              { label: 'Metadata Size', value: 'metadata' }
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
                  <div className={css.section}>
                    <UsageBreakdown
                      data={
                        dataDistribution
                          ? dataDistribution[name]
                            ? dataDistribution[name]
                            : { uncached: true }
                          : undefined
                      }
                      width={
                        dataDistribution && dataDistribution[name]
                          ? `${(dataDistribution[name].total / maxSize) * 100}%`
                          : '0px'
                      }
                    />
                  </div>
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
