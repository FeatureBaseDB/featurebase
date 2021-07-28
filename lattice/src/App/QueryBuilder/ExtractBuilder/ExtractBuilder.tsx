import React, { FC, useState } from 'react';
import InfoIcon from '@material-ui/icons/Info';
import Tooltip from '@material-ui/core/Tooltip';
import { ColumnSelector } from 'App/QueryBuilder/ColumnSelector';
import { Operator, RowGrouping } from 'App/QueryBuilder/rowTypes';
import { RowCallBuilder } from 'App/QueryBuilder/RowCallBuilder';
import { useEffectOnce } from 'react-use';
import css from './ExtractBuilder.module.scss';

type ExtractBuilderProps = {
  table: any;
  query: any;
  showInvalid: boolean;
  onChange: (query: any) => void;
};

export const ExtractBuilder: FC<ExtractBuilderProps> = ({
  table,
  query,
  showInvalid,
  onChange
}) => {
  const { columns, rowCalls } = query
    ? query
    : {
        columns: [],
        rowCalls: []
      };
  const [showColumnSelector, setShowColumnSelector] = useState<boolean>(false);

  useEffectOnce(() => {
    if (!query) {
      onChange({
        columns: table.fields.map((field) => field.name),
        operation: 'Extract',
        rowCalls: []
      });
    }
  });

  return (
    <div className={css.extract}>
      <div className={css.columnsSelector}>
        <span
          className={css.textLink}
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

      <ColumnSelector
        open={showColumnSelector}
        fieldsList={table.fields.map((field) => ({
          name: field.name,
          show: columns ? columns.includes(field.name) : true
        }))}
        onChange={(allColumns) => {
          let cols: string[] = [];
          allColumns.forEach((col) => {
            if (col.show) {
              cols.push(col.name);
            }
          });
          onChange({ ...query, columns: cols });
        }}
        onClose={() => setShowColumnSelector(false)}
      />

      <RowCallBuilder
        rowCalls={rowCalls}
        fields={table.fields}
        showInvalid={showInvalid}
        onChange={(rowCalls: RowGrouping[], operator?: Operator) => {
          onChange({
            ...query,
            rowCalls,
            operator
          });
        }}
      />
    </div>
  );
};
