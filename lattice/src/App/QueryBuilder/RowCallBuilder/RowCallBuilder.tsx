import React, { FC, Fragment, useState } from 'react';
import AddIcon from '@material-ui/icons/Add';
import Button from '@material-ui/core/Button';
import IconButton from '@material-ui/core/IconButton';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import {
  groupOperators,
  Operator,
  RowCallType,
  RowGrouping
} from 'App/QueryBuilder/rowTypes';
import { RowCall } from './RowCall';
import css from './RowCallBuilder.module.scss';

type RowCallBuilderProps = {
  rowCalls: RowGrouping[];
  fields: any[];
  showInvalid: boolean;
  onChange: (rows: RowGrouping[], operator?: Operator) => void;
};

export const RowCallBuilder: FC<RowCallBuilderProps> = ({
  rowCalls = [],
  fields,
  showInvalid,
  onChange
}) => {
  const [newOperatorEl, setNewOperatorEl] = useState<null | HTMLElement>(null);
  const [operatorEl, setOperatorEl] = useState<null | HTMLElement>(null);
  const [operator, setOperator] = useState<Operator>();

  const onNewGroup = (op?: Operator) => {
    const newRow: RowCallType[] = [
      {
        field: '',
        rowOperator: '=',
        value: '',
        type: 'set',
        keys: true
      }
    ];

    onChange([...rowCalls, { row: newRow }], op ? op : operator);
  };

  const onRemoveGroup = (groupIdx: number) => {
    if (groupIdx === 0) {
      onChange(rowCalls.slice(1), operator);
    } else {
      const updatedRowCalls = [...rowCalls];
      updatedRowCalls.splice(groupIdx, 1);
      onChange(updatedRowCalls, operator);

      if (updatedRowCalls.length < 1) {
        setOperator(undefined);
        onChange(updatedRowCalls);
      }
    }
  };

  const onUpdateRow = (
    groupIdx: number,
    newRowData?: RowCallType[],
    rowGroupOperator?: Operator,
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
      } else if (rowGroupOperator) {
        clone.operator = rowGroupOperator;
      }
      updatedRowCalls.splice(groupIdx, 1, clone);
    } else {
      updatedRowCalls.splice(groupIdx, 1);
    }
    onChange(updatedRowCalls, operator);
  };

  return (
    <Fragment>
      {rowCalls?.map((rowCall, idx) => (
        <Fragment key={`group-${idx}`}>
          {idx > 0 ? (
            <Fragment>
              <div className={css.operator}>
                <div className={css.line} />
                <Button
                  className={css.addButton}
                  size="small"
                  onClick={(event) => setOperatorEl(event.currentTarget)}
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
            fields={fields}
            rowData={rowCall.row}
            isNot={rowCall.isNot}
            operator={rowCall.operator}
            showErrors={showInvalid}
            onUpdate={(updatedRow, operator, isNot) =>
              onUpdateRow(idx, updatedRow, operator, isNot)
            }
            onRemoveGroup={() => onRemoveGroup(idx)}
          />
        </Fragment>
      ))}

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
              onNewGroup(op);
              setNewOperatorEl(null);
            }}
          >
            {op}
          </MenuItem>
        ))}
      </Menu>
    </Fragment>
  );
};
