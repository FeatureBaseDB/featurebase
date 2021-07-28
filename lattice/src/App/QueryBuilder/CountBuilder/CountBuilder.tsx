import React, { FC } from 'react';
import { Operator, RowGrouping } from 'App/QueryBuilder/rowTypes';
import { RowCallBuilder } from 'App/QueryBuilder/RowCallBuilder';

type CountBuilderProps = {
  table: any;
  query: any;
  showInvalid: boolean;
  onChange: (query: any) => void;
};

export const CountBuilder: FC<CountBuilderProps> = ({
  table,
  query,
  showInvalid,
  onChange
}) => {
  const { rowCalls } = query ? query : { rowCalls: [] };

  return (
    <div>
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
