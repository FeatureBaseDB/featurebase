import React, { FC, Fragment, useEffect, useState } from 'react';
import ArrowBackIcon from '@material-ui/icons/ArrowBack';
import Button from '@material-ui/core/Button';
import CircularProgress from '@material-ui/core/CircularProgress';
import Divider from '@material-ui/core/Divider';
import Popover from '@material-ui/core/Popover';
import TextField from '@material-ui/core/TextField';
import Typography from '@material-ui/core/Typography';
import {
  cleanupRows,
  stringifyCount,
  stringifyExtract,
  stringifyGroupBy
} from './utils';
import { CountBuilder } from './CountBuilder';
import { ExtractBuilder } from './ExtractBuilder';
import { GroupByBuilder } from './GroupByBuilder';
import { Select } from 'shared/Select';
import css from './QueryBuilder.module.scss';

type QueryBuilderProps = {
  tables: any[];
  savedQuery: number;
  onRun: (
    table: string,
    operation: string,
    query: string,
    countQuery?: string
  ) => void;
  onClear: () => void;
  onExitEdit: () => void;
  onSaveQuery: () => void;
};

export const QueryBuilder: FC<QueryBuilderProps> = ({
  tables,
  savedQuery,
  onRun,
  onClear,
  onExitEdit,
  onSaveQuery
}) => {
  const [saveButtonEl, setSaveButtonEl] = useState<null | HTMLElement>(null);
  const queriesList = JSON.parse(localStorage.getItem('saved-queries') || '[]');
  const [editMode, setEditMode] = useState<boolean>(false);
  const [selectedTable, setSelectedTable] = useState<any>(tables[0]);
  const [operation, setOperation] = useState<string>('Extract');
  const [query, setQuery] = useState<any>({
    table: tables[0].name,
    operation: 'Extract',
    columns: tables[0].fields.map((field) => field.name)
  });
  const [showInvalid, setShowInvalid] = useState<boolean>(false);
  const [updating, setUpdating] = useState<boolean>(false);
  const [queryNameIdx, setQueryNameIdx] = useState<number>(-1);

  useEffect(() => {
    if (savedQuery > -1) {
      const updatedList = JSON.parse(
        localStorage.getItem('saved-queries') || '[]'
      );
      setQuery(updatedList[savedQuery]);
      setEditMode(true);
      setOperation(updatedList[savedQuery].operation);
      const table = tables.find(
        (table) => table.name === updatedList[savedQuery].table
      );
      setSelectedTable(table);
    }
  }, [savedQuery, tables]);

  useEffect(() => {
    if (query.name) {
      setQueryNameIdx(queriesList.findIndex((q) => q.name === query.name));
    }
  }, [query, queriesList]);

  const cleanRows = () => {
    const cleanRows = cleanupRows(query?.rowCalls);
    return {
      ...query,
      rowCalls: cleanRows.cleanRowCalls,
      isInvalid: cleanRows.isInvalid
    };
  };

  const runQuery = () => {
    setShowInvalid(true);
    let queryString = '';
    let countQuery = '';

    switch (operation) {
      case 'Extract':
        const cleanExtractQuery = cleanRows();
        setQuery(cleanExtractQuery);
        const extract = stringifyExtract(cleanExtractQuery);
        if (!extract.error && !cleanExtractQuery.isInvalid) {
          queryString = extract.queryString;
          countQuery = extract.countQuery;
        }
        break;
      case 'GroupBy':
        queryString = stringifyGroupBy(query);
        break;
      case 'Count':
        const cleanCountQuery = cleanRows();
        setQuery(cleanCountQuery);
        const count = stringifyCount(cleanCountQuery);
        if (!count.error && !cleanCountQuery.isInvalid) {
          queryString = count.queryString;
        }
        break;
      default:
        break;
    }

    if (queryString && !query.isInvalid) {
      setShowInvalid(false);
      onRun(selectedTable.name, operation, queryString, countQuery);
    } else {
      setShowInvalid(true);
    }
  };

  const reset = () => {
    setQuery({
      table: selectedTable.name,
      columns: selectedTable.fields.map((field) => field.name),
      operation,
      isInvalid: false
    });
    setShowInvalid(false);
    setEditMode(false);
    setQueryNameIdx(-1);
    onExitEdit();
    onClear();
  };

  const onSave = () => {
    setUpdating(true);
    setTimeout(() => {
      setUpdating(false);
    }, 1500);
    const cleanQuery = cleanRows();
    let queries = [...queriesList];

    if (editMode && queries.length > 0) {
      const clone = [...queries];
      clone.splice(savedQuery, 1, cleanQuery);
      localStorage.setItem('saved-queries', JSON.stringify(clone));
    } else {
      queries.push(cleanQuery);
      localStorage.setItem('saved-queries', JSON.stringify(queries));
    }
    onSaveQuery();
  };

  const renderBuilder = () => {
    switch (operation) {
      case 'Extract':
        return (
          <ExtractBuilder
            table={selectedTable}
            showInvalid={showInvalid}
            query={query}
            onChange={(q) => setQuery(q)}
          />
        );
      case 'GroupBy':
        return (
          <GroupByBuilder
            table={selectedTable}
            showInvalid={showInvalid}
            query={query}
            onChange={(q) => setQuery(q)}
          />
        );
      case 'Count':
        return (
          <CountBuilder
            table={selectedTable}
            showInvalid={showInvalid}
            query={query}
            onChange={(q) => setQuery(q)}
          />
        );
      default:
        return <div>Invalid Operation</div>;
    }
  };

  return (
    <div className={css.builder}>
      <div className={css.sharedConfig}>
        {editMode ? (
          <span className={css.textLink} onClick={reset}>
            <ArrowBackIcon fontSize="inherit" className={css.icon} />
            Back to new query
          </span>
        ) : null}
        <Typography variant="h5" color="textSecondary" paragraph>
          {editMode ? 'Edit Saved Query' : 'New Query'}
        </Typography>

        {editMode ? (
          <div className={css.queryMetadata}>
            <TextField
              variant="outlined"
              label="Query Name"
              placeholder="Unique name to identify query"
              value={query?.name}
              error={
                !query?.name ||
                (queryNameIdx >= 0 && queryNameIdx !== savedQuery)
              }
              helperText={
                queryNameIdx >= 0 && queryNameIdx !== savedQuery
                  ? 'Query name must be unique'
                  : ''
              }
              onChange={(event) =>
                setQuery({ ...query, name: event.target.value })
              }
              margin="normal"
              size="small"
              required
              fullWidth
            />

            <TextField
              variant="outlined"
              label="Description"
              value={query?.description}
              onChange={(event) =>
                setQuery({ ...query, description: event.target.value })
              }
              margin="normal"
              size="small"
              fullWidth
            />
          </div>
        ) : null}

        <Select
          className={css.configSelector}
          label="Table"
          value={selectedTable.name}
          options={tables.map((table) => {
            return { label: table.name, value: table.name };
          })}
          onChange={(value) => {
            const table = tables.find((t) => t.name === value);
            setSelectedTable(table);
            setShowInvalid(false);
            setQuery({
              ...query,
              table: table.name,
              columns: table.fields.map((field) => field.name),
              operation: query.operation,
              rowCalls: [],
              operator: undefined,
              groupByCall: undefined,
              sort: undefined,
              filter: undefined,
              isInvalid: false
            });
          }}
        />

        <Select
          className={css.configSelector}
          label="Operation"
          value={operation}
          options={[
            { label: 'Extract', value: 'Extract' },
            { label: 'Count', value: 'Count' },
            { label: 'GroupBy', value: 'GroupBy' }
          ]}
          onChange={(value) => {
            setOperation(value);
            setShowInvalid(false);
            if (
              ['Extract', 'Count'].includes(operation) &&
              ['Extract', 'Count'].includes(value)
            ) {
              setQuery({
                ...query,
                operation: value
              });
            } else {
              setQuery({
                ...query,
                operation: value,
                columns: selectedTable.fields.map((field) => field.name),
                rowCalls: [],
                operator: undefined,
                groupByCall: undefined,
                sort: undefined,
                filter: undefined,
                isInvalid: false
              });
            }
          }}
        />
        <Divider />
      </div>
      <div className={css.config}>{renderBuilder()}</div>
      <div className={css.builderActions}>
        <div className={css.mainActions}>
          <Button variant="contained" color="primary" onClick={runQuery}>
            Run
          </Button>
          {editMode ? (
            <Button
              variant="contained"
              color="default"
              onClick={onSave}
              disabled={
                !query?.name || (savedQuery >= 0 && queryNameIdx !== savedQuery)
              }
            >
              {updating ? <CircularProgress size="0.875rem" /> : 'Update'}
            </Button>
          ) : (
            <Fragment>
              <Button
                variant="contained"
                color="default"
                onClick={(event) => setSaveButtonEl(event.currentTarget)}
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
                    value={query?.name ? query.name : ''}
                    error={queryNameIdx >= 0}
                    helperText={
                      queryNameIdx >= 0 ? 'Query name must be unique' : ''
                    }
                    onChange={(event) =>
                      setQuery({ ...query, name: event.target.value })
                    }
                    margin="dense"
                    required
                    fullWidth
                  />

                  <TextField
                    variant="outlined"
                    label="Description"
                    value={query?.description ? query.description : ''}
                    onChange={(event) =>
                      setQuery({ ...query, description: event.target.value })
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
                        disabled={!query?.name || queryNameIdx >= 0}
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
        <Button onClick={reset}>Clear</Button>
      </div>
    </div>
  );
};
