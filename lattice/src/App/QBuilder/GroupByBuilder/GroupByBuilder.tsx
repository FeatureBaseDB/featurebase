import React, { FC } from 'react';
import InfoIcon from '@material-ui/icons/Info';
import Tooltip from '@material-ui/core/Tooltip';
import Typography from '@material-ui/core/Typography';
import { GroupBySort } from 'App/QueryBuilder/GroupBySort';
import { SavedQueries } from 'App/QueryBuilder/SavedQueries';
import { Select } from 'shared/Select';
import { stringifyRowData } from 'App/QueryBuilder/stringifyRowData';
import css from './GroupByBuilder.module.scss';

type GroupByBuilderProps = {
  table: any;
  query: any;
  showInvalid: boolean;
  onChange: (query: any) => void;
};

export const GroupByBuilder: FC<GroupByBuilderProps> = ({
  table,
  query,
  showInvalid,
  onChange
}) => {
  const { groupByCall, filter, sort } = query;
  const filters = JSON.parse(
    localStorage.getItem('saved-queries') || '[]'
  ).filter(
    (q) =>
      q.table === table.name &&
      q.operation === 'Extract' &&
      q.rowCalls.length > 0
  );

  return (
    <div className={css.groupBy}>
      <Select
        className={css.fieldSelector}
        label="Primary Field"
        value={
          groupByCall ? (groupByCall.primary ? groupByCall.primary : '') : ''
        }
        options={table.fields
          .filter((f) =>
            ['set', 'time', 'mutex', 'bool', 'int', 'timestamp'].includes(
              f.options.type
            )
          )
          .map((field) => {
            return {
              label: `${field.name} (${field.options.type})`,
              value: field.name
            };
          })}
        onChange={(value) =>
          onChange({
            ...query,
            groupByCall: {
              ...query.groupByCall,
              primary: value.toString()
            }
          })
        }
        error={showInvalid && !groupByCall.primary}
      />

      <Select
        className={css.fieldSelector}
        label="Secondary Field (optional)"
        value={
          groupByCall
            ? groupByCall.secondary
              ? groupByCall.secondary
              : ''
            : ''
        }
        allowEmpty={true}
        options={table.fields
          .filter((f) =>
            ['set', 'time', 'mutex', 'bool', 'int', 'timestamp'].includes(
              f.options.type
            )
          )
          .map((field) => {
            return {
              label: `${field.name} (${field.options.type})`,
              value: field.name
            };
          })}
        onChange={(value) =>
          onChange({
            ...query,
            groupByCall: {
              ...query.groupByCall,
              secondary: value.toString()
            }
          })
        }
      />

      <div className={css.filtersSection}>
        <div className={css.filtersHeader}>
          <Typography variant="caption" color="textSecondary">
            Filter (optional)
            {filters.length > 0 && (
              <Tooltip
                className={css.filtersInfo}
                title={`To use filters, save an Extract query for ${table.name} with at least one field constraint.`}
                placement="top"
                arrow
              >
                <InfoIcon fontSize="inherit" />
              </Tooltip>
            )}
          </Typography>
          {filter ? (
            <span
              className={css.textLink}
              onClick={() => onChange({ ...query, filter: undefined })}
            >
              Clear Filter
            </span>
          ) : null}
        </div>
        {filter ? (
          <Typography variant="caption">{filter}</Typography>
        ) : filters.length > 0 ? (
          <SavedQueries
            queries={filters}
            tables={[table]}
            onClickSaved={(queryIdx) => {
              const { rowCalls, operator } = filters[queryIdx];
              const res = stringifyRowData(rowCalls, operator);
              if (!res.error) {
                onChange({ ...query, filter: res.query });
              }
            }}
          />
        ) : (
          <div className={css.infoMessage}>
            No available filters. To use filters, save an Extract query for{' '}
            {table.name} with at least one field constraint.
          </div>
        )}
      </div>

      <div className={css.sortSection}>
        <Typography variant="caption" color="textSecondary">
          Sort (optional)
        </Typography>
        <GroupBySort
          sort={sort ? sort : []}
          onUpdate={(value) => {
            let isInvalid = false;
            if (
              value.length > 0 &&
              value[0].sortValue.includes('sum') &&
              !value[0].field
            ) {
              isInvalid = true;
            } else if (
              value.length > 1 &&
              value[1].sortValue.includes('sum') &&
              !value[1].field
            ) {
              isInvalid = true;
            }
            onChange({ ...query, sort: value, isInvalid });
          }}
          fields={table.fields
            .filter((field) => field.options.type === 'int')
            .map((field) => {
              return { label: field.name, value: field.name };
            })}
          showErrors={showInvalid}
        />
      </div>
    </div>
  );
};
