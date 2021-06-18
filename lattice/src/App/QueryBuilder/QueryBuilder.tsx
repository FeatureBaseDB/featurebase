import React, { FC, Fragment, useEffect, useState } from 'react';
import AddIcon from '@material-ui/icons/Add';
import ArrowForwardIosIcon from '@material-ui/icons/ArrowForwardIos';
import ArrowBackIcon from '@material-ui/icons/ArrowBack';
import Button from '@material-ui/core/Button';
import CircularProgress from '@material-ui/core/CircularProgress';
import CloseIcon from '@material-ui/icons/Close';
import IconButton from '@material-ui/core/IconButton';
import InfoIcon from '@material-ui/icons/Info';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import Popover from '@material-ui/core/Popover';
import Split from 'react-split';
import TextField from '@material-ui/core/TextField';
import Tooltip from '@material-ui/core/Tooltip';
import Typography from '@material-ui/core/Typography';
import { Block } from 'shared/Block';
import { ColumnSelector } from './ColumnSelector';
import { formatDuration } from 'shared/utils/formatDuration';
import {
  Operator,
  groupOperators,
  RowCallType,
  RowGrouping,
  RowsCallType
} from './rowTypes';
import { getIPRange } from 'get-ip-range';
import { QueryResults } from 'App/Query/QueryResults';
import { ResultType } from 'App/Query/QueryContainer';
import { RowCall } from './RowCall';
import { SavedQueries } from './SavedQueries';
import { Select } from 'shared/Select';
import { stringifyRowData } from './stringifyRowData';
import css from './QueryBuilder.module.scss';

type QueryBuilderProps = {
  tables: any[];
  results?: ResultType;
  fullResultsCount?: number;
  fullRecordsCount?: number;
  error?: ResultType;
  loading: boolean;
  onQuery: (
    table: any,
    operation: string,
    rowData: RowGrouping[],
    columns: string[],
    operator?: Operator
  ) => void;
  onRunGroupBy: (table: any, rowsData: RowsCallType, filter?: string) => void;
  onExternalLookup: (table: string, columns: number[]) => void;
  onClear: () => void;
};

export const QueryBuilder: FC<QueryBuilderProps> = ({
  tables,
  results,
  fullResultsCount,
  fullRecordsCount,
  error,
  loading,
  onQuery,
  onRunGroupBy,
  onExternalLookup,
  onClear
}) => {
  const [newOperatorEl, setNewOperatorEl] = useState<null | HTMLElement>(null);
  const [operatorEl, setOperatorEl] = useState<null | HTMLElement>(null);
  const [saveButtonEl, setSaveButtonEl] = useState<null | HTMLElement>(null);
  const [showBuilder, setShowBuilder] = useState<boolean>(true);
  const [showColumnSelector, setShowColumnSelector] = useState<boolean>(false);
  const [hasInvalid, setHasInvalid] = useState<boolean>(false);
  const [selectedTable, setSelectedTable] = useState<any>(tables[0]);
  const [selectedColumns, setSelectedColumns] = useState<
    { name: string; show: boolean }[]
  >(
    tables[0].fields.map((field) => ({
      name: field.name,
      show: true
    }))
  );
  const [operation, setOperation] = useState<string>('Extract');
  const [groupByCall, setGroupByCall] = useState<RowsCallType>({
    primary: '',
    secondary: ''
  });
  const [rowCalls, setRowCalls] = useState<RowGrouping[]>([]);
  const [operator, setOperator] = useState<Operator>();
  const [editSavedIdx, setEditSavedIdx] = useState<number>(-1);
  const [queryName, setQueryName] = useState<string>('');
  const [queryDescription, setQueryDescription] = useState<string>('');
  const [updating, setUpdating] = useState<boolean>(false);
  const [queriesList, setQueriesList] = useState<any[]>(
    JSON.parse(localStorage.getItem('saved-queries') || '[]')
  );
  const [groupByFilters, setGroupByFilters] = useState<any[]>([]);
  const [filter, setFilter] = useState<string>();
  const colSizes = JSON.parse(
    localStorage.getItem('builderColSizes') || '[25, 75]'
  );
  const queryNameIdx = queriesList.findIndex((q) => q.name === queryName);
  const inEditMode = editSavedIdx >= 0;

  useEffect(() => {
    setGroupByFilters(
      queriesList.filter(
        (q) =>
          q.table === selectedTable.name &&
          q.operation === 'Extract' &&
          q.rowCalls.length > 0
      )
    );
  }, [selectedTable, queriesList]);

  const getColumnsToShow = (cols?: { name: string; show: boolean }[]) => {
    let columns: string[] = [];
    const list = cols ? cols : selectedColumns;
    list.forEach((field) => {
      if (field.show) {
        columns.push(field.name);
      }
    });

    return columns;
  };

  const onChangeColumns = (cols) => {
    setSelectedColumns(cols);
    if (results) {
      const { cleanRowCalls, isInvalid } = cleanupRows();

      setHasInvalid(isInvalid);
      setRowCalls(cleanRowCalls);
      if (!isInvalid) {
        let columns: string[] = [];
        if (operation === 'Extract') {
          columns = getColumnsToShow(cols);
        }
        onQuery(selectedTable, operation, cleanRowCalls, columns, operator);
      }
    }
  };

  const onNewGroup = () => {
    const newRow: RowCallType[] = [
      {
        field: '',
        rowOperator: '=',
        value: '',
        type: 'set',
        keys: true
      }
    ];

    setRowCalls([...rowCalls, { row: newRow }]);
  };

  const onRemoveGroup = (groupIdx: number) => {
    if (groupIdx === 0) {
      setRowCalls(rowCalls.slice(1));
    } else {
      const updatedRowCalls = [...rowCalls];
      updatedRowCalls.splice(groupIdx, 1);
      setRowCalls(updatedRowCalls);

      if (updatedRowCalls.length < 1) {
        setOperator(undefined);
      }
    }
  };

  const onUpdateRow = (
    groupIdx: number,
    newRowData?: RowCallType[],
    operator?: Operator,
    isNot?: boolean
  ) => {
    const updatedRowCalls = [...rowCalls];
    if (newRowData) {
      const clone: RowGrouping = {
        ...rowCalls[groupIdx],
        row: newRowData,
        isNot
      };
      if (newRowData.length <= 1) {
        delete clone.operator;
      } else if (operator) {
        clone.operator = operator;
      }
      updatedRowCalls.splice(groupIdx, 1, clone);
    } else {
      updatedRowCalls.splice(groupIdx, 1);
    }
    setRowCalls(updatedRowCalls);
  };

  const cleanupRows = () => {
    // remove empty groups and row calls
    let isInvalid = false;
    let cleanRowCalls: RowGrouping[] = [];
    let cleanGroups: RowGrouping[] = [];

    rowCalls.forEach((group, groupIdx) => {
      cleanGroups.push({ ...group, row: [] });
      group.row.forEach((row) => {
        const { field, value, rowOperator } = row;
        if (field || value) {
          cleanGroups[groupIdx].row.push(row);

          if (!field || !value) {
            isInvalid = true;
          } else if (rowOperator === 'cidr') {
            try {
              getIPRange(value);
            } catch (err) {
              isInvalid = true;
            }
          }
        }
      });
    });

    cleanGroups.forEach((group) => {
      if (group.row.length > 0) {
        cleanRowCalls.push(group);
      }
    });

    return { cleanRowCalls, isInvalid };
  };

  const onRunClick = () => {
    if (operation === 'GroupBy') {
      const isInvalid = groupByCall.primary ? false : true;
      setHasInvalid(isInvalid);
      if (!isInvalid) {
        onRunGroupBy(selectedTable, groupByCall, filter);
      }
    } else {
      const { cleanRowCalls, isInvalid } = cleanupRows();

      setHasInvalid(isInvalid);
      setRowCalls(cleanRowCalls);
      if (!isInvalid) {
        let columns: string[] = [];
        if (operation === 'Extract') {
          columns = getColumnsToShow();
        }
        onQuery(selectedTable, operation, cleanRowCalls, columns, operator);
      }
    }
  };

  const onSave = () => {
    setUpdating(true);
    setTimeout(() => {
      setUpdating(false);
    }, 1500);
    const { cleanRowCalls, isInvalid } = cleanupRows();
    let queries = [...queriesList];
    const queryObj = {
      name: queryName,
      description: queryDescription,
      table: selectedTable.name,
      operation,
      columns: operation === 'Extract' ? getColumnsToShow() : undefined,
      operator,
      rowCalls: cleanRowCalls,
      groupByCall,
      filter,
      isInvalid
    };

    if (inEditMode && queries.length > 0) {
      const clone = [...queries];
      clone.splice(editSavedIdx, 1, queryObj);
      localStorage.setItem('saved-queries', JSON.stringify(clone));
      setQueriesList(clone);
    } else {
      queries.push(queryObj);
      localStorage.setItem('saved-queries', JSON.stringify(queries));
      setQueriesList(queries);
      setEditSavedIdx(queries.length - 1);
    }
  };

  const onExportLogs = () => {
    if (results) {
      const columns = results.rows.map((row) => row[0].uint64val);
      const table = results.index ? results.index : '';
      onExternalLookup(table, columns);
    }
  };

  const onSavedQueryClick = (idx: number) => {
    const {
      name,
      description,
      table,
      operation,
      columns,
      operator,
      rowCalls,
      groupByCall,
      filter,
      isInvalid
    } = queriesList[idx];
    const tableDetails = tables.find((t) => t.name === table);
    setEditSavedIdx(idx);
    setSelectedTable(tableDetails);
    if (columns) {
      setSelectedColumns(
        tableDetails.fields.map((field) =>
          columns.includes(field.name)
            ? { name: field.name, show: true }
            : { name: field.name, show: false }
        )
      );
    }
    setOperation(operation);
    setOperator(operator);
    setRowCalls(rowCalls);
    setGroupByCall(groupByCall);
    setQueryName(name);
    setQueryDescription(description || '');
    setHasInvalid(!!isInvalid);
    setFilter(filter);

    if (!isInvalid) {
      if (operation === 'GroupBy') {
        onRunGroupBy(tableDetails, groupByCall, filter);
      } else {
        onQuery(tableDetails, operation, rowCalls, columns, operator);
      }
    }
  };

  const onRemoveQuery = (queryIdx: number) => {
    let queries = [...queriesList];
    queries.splice(queryIdx, 1);
    localStorage.setItem('saved-queries', JSON.stringify(queries));
    setQueriesList(queries);
  };

  const reset = () => {
    setEditSavedIdx(-1);
    setSelectedTable(tables[0]);
    setSelectedColumns(
      tables[0].fields.map((field) => ({
        name: field.name,
        show: true
      }))
    );
    setOperation('Extract');
    setRowCalls([]);
    setQueryName('');
    setQueryDescription('');
    setHasInvalid(false);
    setGroupByCall({
      primary: '',
      secondary: ''
    });
    setFilter(undefined);
    onClear();
  };

  return (
    <Split
      sizes={showBuilder ? colSizes : [0, 100]}
      cursor="col-resize"
      minSize={showBuilder ? 350 : 50}
      onDragEnd={(sizes) =>
        localStorage.setItem('builderColSizes', JSON.stringify(sizes))
      }
      gutter={(_index, direction) => {
        const gutter = document.createElement('div');
        gutter.className = `gutter gutter-${direction}`;
        const dragbars = document.createElement('div');
        dragbars.className = 'dragBar';
        gutter.appendChild(dragbars);
        return gutter;
      }}
      style={{ display: 'flex' }}
      className={!showBuilder ? 'hide-gutter' : undefined}
    >
      {showBuilder ? (
        <div className={css.builderColumn}>
          <div className={css.openClose}>
            <IconButton onClick={() => setShowBuilder(false)} size="small">
              <CloseIcon />
            </IconButton>
          </div>
          <div className={css.builderHeader}>
            {inEditMode ? (
              <span className={css.link} onClick={reset}>
                <ArrowBackIcon fontSize="inherit" className={css.icon} />
                Back to new query
              </span>
            ) : null}
            <Typography variant="h5" color="textSecondary">
              {inEditMode ? 'Edit Saved Query' : 'New Query'}
            </Typography>
          </div>
          <div className={css.builder}>
            {inEditMode ? (
              <div className={css.queryMetadata}>
                <TextField
                  variant="outlined"
                  label="Query Name"
                  placeholder="Unique name to identify query"
                  value={queryName}
                  error={
                    !queryName ||
                    (queryNameIdx >= 0 && queryNameIdx !== editSavedIdx)
                  }
                  helperText={
                    queryNameIdx >= 0 && queryNameIdx !== editSavedIdx
                      ? 'Query name must be unique'
                      : ''
                  }
                  onChange={(event) => setQueryName(event.target.value)}
                  margin="normal"
                  size="small"
                  required
                  fullWidth
                />

                <TextField
                  variant="outlined"
                  label="Description"
                  value={queryDescription}
                  onChange={(event) => setQueryDescription(event.target.value)}
                  margin="normal"
                  size="small"
                  fullWidth
                />
              </div>
            ) : null}
            <div className={css.tableSelector}>
              <Select
                label="Table"
                value={selectedTable.name}
                options={tables.map((table) => {
                  return { label: table.name, value: table.name };
                })}
                onChange={(value) => {
                  const table = tables.find((t) => t.name === value);
                  setSelectedTable(table);
                  setSelectedColumns(
                    table.fields.map((field) => ({
                      name: field.name,
                      show: true
                    }))
                  );
                  setRowCalls([]);
                  setOperator(undefined);
                  setGroupByCall({
                    primary: '',
                    secondary: ''
                  });
                }}
              />
            </div>

            <div className={css.operationSelector}>
              <Select
                label="Operation"
                value={operation}
                options={[
                  { label: 'Extract', value: 'Extract' },
                  { label: 'Count', value: 'Count' },
                  { label: 'GroupBy', value: 'GroupBy' }
                ]}
                onChange={(value) => setOperation(value)}
              />

              {operation === 'Extract' ? (
                <div className={css.columnsSelector}>
                  <span
                    className={css.link}
                    onClick={() => setShowColumnSelector(true)}
                  >
                    Configure result fields
                  </span>
                  <Tooltip
                    className={css.info}
                    title="This controls the fields that will show up in the results table after querying"
                    placement="top"
                    arrow
                  >
                    <InfoIcon fontSize="inherit" />
                  </Tooltip>
                </div>
              ) : null}

              <ColumnSelector
                open={showColumnSelector}
                fieldsList={selectedColumns}
                onChange={onChangeColumns}
                onClose={() => setShowColumnSelector(false)}
              />
            </div>

            {selectedTable && operation !== 'GroupBy'
              ? rowCalls.map((rowCall, idx) => (
                  <Fragment key={`group-${idx}`}>
                    {idx > 0 ? (
                      <Fragment>
                        <div className={css.operator}>
                          <div className={css.line} />
                          <Button
                            className={css.addButton}
                            size="small"
                            onClick={(event) =>
                              setOperatorEl(event.currentTarget)
                            }
                          >
                            {operator}
                          </Button>
                          <div className={css.line} />
                        </div>
                        <Menu
                          open={!!operatorEl}
                          anchorEl={operatorEl}
                          classes={{ paper: css.menuPaper }}
                          onClose={() => setOperatorEl(null)}
                        >
                          {groupOperators.map((op: Operator) => (
                            <MenuItem
                              key={`${idx}-${op}`}
                              onClick={() => {
                                setOperator(op);
                                setOperatorEl(null);
                              }}
                            >
                              {op}
                            </MenuItem>
                          ))}
                        </Menu>
                      </Fragment>
                    ) : null}
                    <RowCall
                      fields={selectedTable.fields}
                      rowData={rowCall.row}
                      isNot={rowCall.isNot}
                      operator={rowCall.operator}
                      showErrors={hasInvalid}
                      onUpdate={(updatedRow, operator, isNot) =>
                        onUpdateRow(idx, updatedRow, operator, isNot)
                      }
                      onRemoveGroup={() => onRemoveGroup(idx)}
                    />
                  </Fragment>
                ))
              : null}

            {selectedTable && operation === 'GroupBy' ? (
              <div className={css.groupBy}>
                <div className={css.line} />
                <Select
                  className={css.fieldSelector}
                  label="Primary Field"
                  value={groupByCall.primary}
                  options={selectedTable.fields
                    .filter((f) =>
                      [
                        'set',
                        'time',
                        'mutex',
                        'bool',
                        'int',
                        'timestamp'
                      ].includes(f.options.type)
                    )
                    .map((field) => {
                      return {
                        label: `${field.name} (${field.options.type})`,
                        value: field.name
                      };
                    })}
                  onChange={(value) =>
                    setGroupByCall({
                      ...groupByCall,
                      primary: value.toString()
                    })
                  }
                  error={hasInvalid && !groupByCall.primary}
                />
                <Select
                  className={css.fieldSelector}
                  label="Secondary Field (optional)"
                  value={groupByCall.secondary}
                  allowEmpty={true}
                  options={selectedTable.fields
                    .filter((f) =>
                      [
                        'set',
                        'time',
                        'mutex',
                        'bool',
                        'int',
                        'timestamp'
                      ].includes(f.options.type)
                    )
                    .map((field) => {
                      return {
                        label: `${field.name} (${field.options.type})`,
                        value: field.name
                      };
                    })}
                  onChange={(value) =>
                    setGroupByCall({
                      ...groupByCall,
                      secondary: value.toString()
                    })
                  }
                />

                <div className={css.filterHeader}>
                  <Typography variant="caption" color="textSecondary">
                    Filter (optional)
                    {groupByFilters.length > 0 && (
                      <Tooltip
                        className={css.filterInfo}
                        title={`To use filters, save an Extract query for ${selectedTable.name} with at least one field constraint.`}
                        placement="top"
                        arrow
                      >
                        <InfoIcon fontSize="inherit" />
                      </Tooltip>
                    )}
                  </Typography>
                  {filter ? (
                    <span
                      className={css.link}
                      onClick={() => setFilter(undefined)}
                    >
                      Clear Filter
                    </span>
                  ) : null}
                </div>
                {filter ? (
                  <Typography variant="caption">{filter}</Typography>
                ) : groupByFilters.length > 0 ? (
                  <SavedQueries
                    queries={groupByFilters}
                    tables={tables}
                    onClickSaved={(queryIdx) => {
                      const { rowCalls, operator } = groupByFilters[queryIdx];
                      const res = stringifyRowData(rowCalls, operator);
                      if (!res.error) {
                        setFilter(res.query);
                      }
                    }}
                  />
                ) : (
                  <div className={css.infoMessage}>
                    No available filters. To use filters, save an Extract query
                    for {selectedTable.name} with at least one field constraint.
                  </div>
                )}
              </div>
            ) : (
              <div className={css.newGroup}>
                <div className={css.line} />
                <IconButton
                  className={css.addButton}
                  size="small"
                  onClick={(event) => {
                    if (rowCalls.length === 0 || !!operator) {
                      onNewGroup();
                    } else {
                      setNewOperatorEl(event.currentTarget);
                    }
                  }}
                >
                  <AddIcon fontSize="inherit" />
                </IconButton>
                <div className={css.line} />
              </div>
            )}
            <Menu
              open={!!newOperatorEl}
              anchorEl={newOperatorEl}
              classes={{ paper: css.menuPaper }}
              onClose={() => setNewOperatorEl(null)}
            >
              {groupOperators.map((op: Operator) => (
                <MenuItem
                  key={`operator-${op}`}
                  onClick={() => {
                    setOperator(op);
                    onNewGroup();
                    setNewOperatorEl(null);
                  }}
                >
                  {op}
                </MenuItem>
              ))}
            </Menu>
          </div>
          <div className={css.builderActions}>
            <div className={css.mainActions}>
              <Button variant="contained" color="primary" onClick={onRunClick}>
                Run
              </Button>
              {inEditMode ? (
                <Button
                  variant="contained"
                  color="default"
                  onClick={onSave}
                  disabled={
                    !queryName ||
                    (queryNameIdx >= 0 && queryNameIdx !== editSavedIdx)
                  }
                >
                  {updating ? <CircularProgress size="0.875rem" /> : 'Update'}
                </Button>
              ) : (
                <Fragment>
                  <Button
                    variant="contained"
                    color="default"
                    onClick={(event) => {
                      setSaveButtonEl(event.currentTarget);
                      setQueryName('');
                      setQueryDescription('');
                    }}
                  >
                    Save
                  </Button>
                  <Popover
                    open={!!saveButtonEl}
                    anchorEl={saveButtonEl}
                    onClose={() => setSaveButtonEl(null)}
                    anchorOrigin={{
                      vertical: 'top',
                      horizontal: 'left'
                    }}
                    transformOrigin={{
                      vertical: 'bottom',
                      horizontal: 'left'
                    }}
                  >
                    <div className={css.savePopover}>
                      <Typography variant="caption" paragraph>
                        Give your query a unique name and a short description to
                        help you identify it for future use.
                      </Typography>

                      <TextField
                        variant="outlined"
                        label="Query Name"
                        placeholder="Unique name to identify query"
                        value={queryName}
                        error={queryNameIdx >= 0}
                        helperText={
                          queryNameIdx >= 0 ? 'Query name must be unique' : ''
                        }
                        onChange={(event) => setQueryName(event.target.value)}
                        margin="dense"
                        required
                        fullWidth
                      />

                      <TextField
                        variant="outlined"
                        label="Description"
                        value={queryDescription}
                        onChange={(event) =>
                          setQueryDescription(event.target.value)
                        }
                        margin="dense"
                        fullWidth
                      />

                      <div className={css.popoverActions}>
                        <div className={css.saveActions}>
                          <Button
                            variant="contained"
                            color="default"
                            onClick={() => {
                              onSave();
                              setSaveButtonEl(null);
                            }}
                            disabled={!queryName || queryNameIdx >= 0}
                          >
                            Ok
                          </Button>
                          <Button
                            color="default"
                            onClick={() => setSaveButtonEl(null)}
                          >
                            Cancel
                          </Button>
                        </div>
                      </div>
                    </div>
                  </Popover>
                </Fragment>
              )}
            </div>
            <Button
              onClick={() => {
                setRowCalls([]);
                setHasInvalid(false);
                setEditSavedIdx(-1);
                setSelectedColumns(
                  selectedTable.fields.map((field) => ({
                    name: field.name,
                    show: true
                  }))
                );
                setGroupByCall({
                  primary: '',
                  secondary: ''
                });
                setFilter(undefined);
                onClear();
              }}
            >
              Clear
            </Button>
          </div>
        </div>
      ) : (
        <div className={css.collapsedBuilder}>
          <IconButton onClick={() => setShowBuilder(true)} size="small">
            <ArrowForwardIosIcon />
          </IconButton>
        </div>
      )}
      <div className={css.results}>
        <Block className={css.resultsBlock}>
          <div className={css.resultsHeader}>
            <Typography variant="h5" color="textSecondary">
              Results{' '}
            </Typography>
            {results?.query.includes('Extract(') ? (
              <div className={css.download}>
                <Tooltip
                  className={css.downloadInfo}
                  title="Download raw data from query results"
                  placement="top"
                  arrow
                >
                  <InfoIcon fontSize="inherit" />
                </Tooltip>
                <Button onClick={onExportLogs}>Download</Button>
              </div>
            ) : null}
          </div>
          {loading ? <div>Loading...</div> : null}
          {results && !error && !loading ? (
            <Fragment>
              {fullResultsCount && fullRecordsCount ? (
                <div className={css.infoMessage}>
                  {results.duration ? (
                    <div>
                      {fullRecordsCount.toLocaleString()} records scanned in{' '}
                      {formatDuration(results.duration, true)}.
                    </div>
                  ) : null}
                  <div>
                    Showing{' '}
                    {fullResultsCount > 1000 ? 'first 1,000 rows of' : 'all'}{' '}
                    {fullResultsCount.toLocaleString()} results.
                  </div>
                </div>
              ) : null}
              <QueryResults results={results} />
            </Fragment>
          ) : null}
          {error && !loading ? <div>{error.error}</div> : null}
          {!loading && !results && !error ? (
            <Fragment>
              <Typography variant="caption" color="textSecondary">
                Build a query to see results
              </Typography>
              {queriesList.length > 0 ? (
                <div className={css.savedQueries}>
                  <Typography variant="h5" color="textSecondary">
                    Saved Queries
                  </Typography>
                  <SavedQueries
                    queries={queriesList}
                    tables={tables}
                    onClickSaved={onSavedQueryClick}
                    onRemoveQuery={onRemoveQuery}
                  />
                </div>
              ) : null}
            </Fragment>
          ) : null}
        </Block>
      </div>
    </Split>
  );
};
