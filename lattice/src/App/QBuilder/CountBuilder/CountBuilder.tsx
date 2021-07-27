import React, { FC } from 'react';
import { RowCallBuilder } from 'App/QBuilder/RowCallBuilder';

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
        onChange={(rowCalls, operator, isInvalid) => {
          onChange({
            ...query,
            rowCalls,
            operator,
            isInvalid
          });
        }}
      />
    </div>
  );
};
