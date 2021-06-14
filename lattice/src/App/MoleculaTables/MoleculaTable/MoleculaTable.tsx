import React, { FC, Fragment, useState, useEffect } from 'react';
import ArrowDropDownIcon from '@material-ui/icons/ArrowDropDown';
import Breadcrumbs from '@material-ui/core/Breadcrumbs';
import classNames from 'classnames';
import Fuse from 'fuse.js';
import Highlighter from 'react-highlight-words';
import isEmpty from 'lodash/isEmpty';
import Link from '@material-ui/core/Link';
import map from 'lodash/map';
import moment from 'moment';
import OrderBy from 'lodash/orderBy';
import Reduce from 'lodash/reduce';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import TextField from '@material-ui/core/TextField';
import Tooltip from '@material-ui/core/Tooltip';
import Typography from '@material-ui/core/Typography';
import { Block } from 'shared/Block';
import { Pager } from 'shared/Pager';
import { UsageBreakdown } from '../UsageBreakdown';
import css from './MoleculaTable.module.scss';

type MoleculaTableProps = {
  table: any;
  dataDistribution: any;
  lastUpdated: string;
};

export const MoleculaTable: FC<MoleculaTableProps> = ({
  table,
  dataDistribution,
  lastUpdated
}) => {
  const [page, setPage] = useState<number>(1);
  const [resultsPerPage, setResultsPerPage] = useState<number>(10);
  const sliceStart = (page - 1) * resultsPerPage;
  const [searchText, setSearchText] = useState<string>('');
  const [filteredFields, setFiltereedFields] = useState(table.fields);
  const [fieldsData, setFieldsData] = useState<{}>({});
  const [maxFieldSize, setMaxFieldSize] = useState<number>(0);
  const [sort, setSort] = useState<string>('total');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');
  const lastUpdatedMoment = moment(lastUpdated).utc();

  useEffect(() => {
    if (dataDistribution && !dataDistribution.uncached) {
      const aggregatedFieldsData = Reduce(
        dataDistribution.fields,
        (result, value) => {
          let newResult = {};
          const keys = Object.keys(value);
          keys.forEach(
            (key) =>
              (newResult[key] = {
                total: result[key].total + value[key].total,
                fragments: result[key].fragments + value[key].fragments,
                keys: result[key].keys + value[key].keys,
                metadata: result[key].metadata + value[key].metadata
              })
          );
          return newResult;
        }
      );

      const sorted = OrderBy(aggregatedFieldsData, ['total'], ['desc']);
      if (sorted.length > 0) {
        setMaxFieldSize(sorted[0].total);
      }

      setFieldsData(aggregatedFieldsData);
    }
  }, [dataDistribution]);

  useEffect(() => {
    if (searchText.length > 1) {
      const fuse = new Fuse(table.fields, {
        keys: ['name'],
        minMatchCharLength: 2,
        ignoreLocation: true,
        threshold: 0
      });
      const result = fuse.search(searchText);

      let resultsArray: any[] = [];
      result.forEach((r: any) => {
        resultsArray.push({ ...r?.item, ...fieldsData[r?.item.name] });
      });
      setFiltereedFields(OrderBy(resultsArray, [sort], [sortDir]));
      setPage(1);
    } else {
      const aggregatedData = table.fields.map((field) => {
        return { ...field, ...fieldsData[field.name] };
      });
      setFiltereedFields(OrderBy(aggregatedData, [sort], [sortDir]));
    }
  }, [searchText, table.fields, sort, sortDir, fieldsData]);

  const renderValue = (value: string) => {
    let valueString = value.toString();
    const isNumber = !isNaN(Number(value));
    if (isNumber) {
      const numValue = Number(value);
      return numValue.toLocaleString();
    }

    return valueString;
  };

  const onSortClick = (name: string) => {
    if (sort === name) {
      setSortDir(sortDir === 'desc' ? 'asc' : 'desc');
    } else {
      setSort(name);
      setSortDir('asc');
    }
  };

  return (
    <Block>
      <Breadcrumbs aria-label="breadcrumb">
        <Link color="inherit" variant="caption" href="/tables">
          Tables
        </Link>
        <Typography color="textPrimary" variant="caption">
          {table.name}
        </Typography>
      </Breadcrumbs>
      <Typography variant="h5" color="textSecondary">
        {table.name}
      </Typography>
      <div className={css.infoMessage}>
        {dataDistribution && dataDistribution.uncached ? (
          'Disk usage will be calculated after the next cache refresh.'
        ) : (
          <Fragment>
            Disk usage last updated{' '}
            <Tooltip
              title={`${lastUpdatedMoment.format('M/D/YYYY hh:mm a')} UTC`}
              placement="top"
              arrow
            >
              <span className={css.relativeTime}>{lastUpdatedMoment.fromNow()}</span>
            </Tooltip>
            .
          </Fragment>
        )}
      </div>
      <div className={css.layout}>
        <div>
          <label className={css.label}>keys</label>
          <div>
            <code className={css.code}>
              {table.options.keys ? 'TRUE' : 'FALSE'}
            </code>
          </div>
        </div>
        <div className={css.breakdown}>
          <UsageBreakdown data={dataDistribution} />
        </div>
      </div>
      <div>
        <div>
          <label className={css.label}>Fields</label>
          {searchText.length > 1 && (
            <Typography className={css.clearFilter} variant="caption">
              <span className={css.reset} onClick={() => setSearchText('')}>
                Clear filter
              </span>
            </Typography>
          )}
        </div>
        <div className={css.filter}>
          <TextField
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            placeholder="Search for Field"
            variant="outlined"
            size="small"
            fullWidth
          />
        </div>
        <Table size="small" className={css.table}>
          <TableHead>
            <TableRow>
              <TableCell className={css.tableHeader}>
                <span
                  className={classNames(css.sortable, {
                    [css.currentSort]: sort === 'name'
                  })}
                  onClick={() => onSortClick('name')}
                >
                  Name{' '}
                  <ArrowDropDownIcon
                    className={classNames(css.sortArrow, {
                      [css.asc]: sortDir === 'asc'
                    })}
                  />
                </span>
              </TableCell>
              <TableCell className={css.tableHeader}>Type</TableCell>
              <TableCell className={css.tableHeader}>Cardinality</TableCell>
              <TableCell className={css.tableHeader}>Options</TableCell>
              <TableCell className={css.tableHeader}>
                <span
                  className={classNames(css.sortable, {
                    [css.currentSort]: sort === 'total'
                  })}
                  onClick={() => onSortClick('total')}
                >
                  Disk Usage{' '}
                  <ArrowDropDownIcon
                    className={classNames(css.sortArrow, {
                      [css.asc]: sortDir === 'asc'
                    })}
                  />
                </span>
              </TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {filteredFields
              .slice(sliceStart, sliceStart + resultsPerPage)
              .map((field) => {
                const { name, options, cardinality } = field;
                const { type, keys, bitDepth, ...rest } = options;
                const showKeys = ['set', 'time', 'mutex'].includes(type);

                return (
                  <TableRow key={`field-${name}`} className={css.row}>
                    <TableCell className={css.tableCell}>
                      <Highlighter
                        highlightClassName={css.highlight}
                        searchWords={searchText.length > 1 ? [searchText] : []}
                        textToHighlight={name}
                        autoEscape={true}
                      />
                    </TableCell>
                    <TableCell className={css.tableCell}>
                      <code className={css.code}>
                        {type} {showKeys ? (keys ? '(keys)' : '(ID)') : null}
                      </code>
                    </TableCell>
                    <TableCell className={css.tableCell}>
                      {cardinality ? cardinality.toLocaleString() : '-'}
                    </TableCell>
                    <TableCell className={css.tableCell}>
                      <div className={css.optionsTable}>
                        {map(rest, (value, key) => {
                          if (value !== '') {
                            const isMinMax = key === 'min' || key === 'max';
                            const scale = rest.scale
                              ? Math.pow(10, rest.scale)
                              : 1;
                            const max = 9223372036854776000;
                            const isMaxed = max / scale === Math.abs(value);
                            if (isMinMax && isMaxed) {
                              return null;
                            } else {
                              return (
                                <Fragment key={`field-${name}-${key}`}>
                                  <code>{key}</code>
                                  <code>|</code>
                                  <code>{renderValue(value)}</code>
                                </Fragment>
                              );
                            }
                          }
                          return null;
                        })}
                      </div>
                    </TableCell>
                    <TableCell className={css.tableCell}>
                      <UsageBreakdown
                        data={
                          isEmpty(field)
                            ? field
                            : dataDistribution
                            ? dataDistribution.uncached
                              ? dataDistribution
                              : field
                            : field
                        }
                        width={`${(field.total / maxFieldSize) * 150}px`}
                        showLabel={false}
                        usageValueSize="small"
                      />
                    </TableCell>
                  </TableRow>
                );
              })}
          </TableBody>
        </Table>
        {filteredFields.length > 0 && (
          <div className={css.pagination}>
            <Pager
              page={page}
              rowsPerPage={resultsPerPage}
              totalItems={filteredFields.length}
              showTotal={true}
              onChangePage={setPage}
              onChangePerPage={setResultsPerPage}
            />
          </div>
        )}
        {filteredFields.length === 0 && (
          <Typography
            className={css.noResults}
            variant="caption"
            color="textSecondary"
            component="div"
          >
            No fields to show.
            {searchText && (
              <span className={css.reset} onClick={() => setSearchText('')}>
                Clear filter
              </span>
            )}
          </Typography>
        )}
      </div>
    </Block>
  );
};
