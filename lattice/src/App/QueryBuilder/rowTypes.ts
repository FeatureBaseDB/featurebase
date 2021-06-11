export type Operator = 'and' | 'or';

export const groupOperators: Operator[] = ['and', 'or'];

export type RowGrouping = {
  row: RowCallType[];
  isNot?: boolean;
  operator?: Operator;
}

export type RowCallType = {
  field: string;
  rowOperator: string;
  value: string;
  type: string;
  keys?: boolean;
};

export type RowsCallType = {
  primary: string;
  secondary: string;
};
