import React, { FC, Fragment, useState } from 'react';
import CheckBoxIcon from '@material-ui/icons/CheckBox';
import CheckBoxOutlineBlankIcon from '@material-ui/icons/CheckBoxOutlineBlank';
import classNames from 'classnames';
import CloseIcon from '@material-ui/icons/Close';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import moment from 'moment';
import TextField from '@material-ui/core/TextField';
import Typography from '@material-ui/core/Typography';
import { getIPRange } from 'get-ip-range';
import { operators } from './helpers';
import { groupOperators, Operator, RowCallType } from 'App/QueryBuilder/rowTypes';
import { Select } from 'shared/Select';
import MomentUtils from '@date-io/moment';
import { MuiPickersUtilsProvider, DateTimePicker } from '@material-ui/pickers';
import css from './RowCall.module.scss';

type RowCallProps = {
  fields: any[];
  rowData: RowCallType[];
  isNot?: boolean;
  operator?: Operator;
  showErrors: boolean;
  onUpdate: (
    updatedRow: RowCallType[],
    operator?: Operator,
    isNot?: boolean
  ) => void;
  onRemoveGroup: () => void;
};

export const RowCall: FC<RowCallProps> = ({
  fields,
  rowData,
  isNot,
  operator,
  showErrors,
  onUpdate,
  onRemoveGroup
}) => {
  const [operatorEl, setOperatorEl] = useState<null | HTMLElement>(null);
  const [activeOperatorEl, setActiveOperatorEl] =
    useState<null | HTMLElement>(null);
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const [activeAnchor, setActiveAnchor] = useState<number>();

  const onNewRow = (op?: Operator) => {
    const newRow: RowCallType = {
      field: '',
      rowOperator: '=',
      value: '',
      type: 'set',
      keys: true
    };

    if (op) {
      onUpdate([...rowData, newRow], op, isNot);
    } else {
      onUpdate([...rowData, newRow], undefined, isNot);
    }
  };

  const onRowUpdate = (rowIdx: number, data?: RowCallType) => {
    const updatedRows = [...rowData];
    if (data) {
      updatedRows.splice(rowIdx, 1, data);
    } else {
      updatedRows.splice(rowIdx, 1);
    }
    onUpdate(updatedRows, operator, isNot);
  };

  return (
    <div className={css.rowGroup}>
      <div className={css.removeGroup}>
        <CloseIcon
          fontSize="inherit"
          className={css.removeGroupIcon}
          onClick={onRemoveGroup}
        />
      </div>

      <div className={css.isNot}>
        {isNot ? (
          <CheckBoxIcon
            fontSize="small"
            className={classNames(css.isNotCheckbox, css.checked)}
            onClick={() => onUpdate(rowData, operator, !isNot)}
          />
        ) : (
          <CheckBoxOutlineBlankIcon
            fontSize="small"
            className={css.isNotCheckbox}
            onClick={() => onUpdate(rowData, operator, !isNot)}
          />
        )}
        <Typography
          variant="caption"
          color="textSecondary"
          className={css.isNotLabel}
          onClick={() => onUpdate(rowData, operator, !isNot)}
        >
          Not
        </Typography>
      </div>

      {rowData.map((row, idx) => {
        const { field, rowOperator, value, keys } = row;
        const type = ['set', 'time', 'mutex'].includes(row.type)
          ? `${row.type}-${keys ? 'keys' : 'id'}`
          : row.type;
        let isInvalidValue = !value;
        if (rowOperator === 'cidr') {
          try {
            getIPRange(value);
          } catch (err) {
            isInvalidValue = true;
          }
        }

        return (
          <Fragment key={`row-${idx}`}>
            {idx > 0 && operator ? (
              <div>
                <span
                  className={css.link}
                  onClick={(event) => setActiveOperatorEl(event.currentTarget)}
                >
                  {operator}
                </span>
                <Menu
                  open={!!activeOperatorEl}
                  anchorEl={activeOperatorEl}
                  classes={{ paper: css.menuPaper }}
                  onClose={() => setActiveOperatorEl(null)}
                >
                  {groupOperators.map((op: Operator) => (
                    <MenuItem
                      key={op}
                      onClick={() => {
                        onUpdate(rowData, op);
                        setActiveOperatorEl(null);
                      }}
                    >
                      {op}
                    </MenuItem>
                  ))}
                </Menu>
              </div>
            ) : null}
            <div className={css.row}>
              <Select
                className={css.fieldSelector}
                label="Field"
                value={field}
                error={showErrors && !field}
                fullWidth={false}
                options={fields.map((field) => {
                  return {
                    label: `${field.name} (${field.options.type})`,
                    value: field.name
                  };
                })}
                onChange={(value) => {
                  const updatedField = fields.find((f) => f.name === value);
                  const rowUpdate =
                    row.type === updatedField.options.type
                      ? {
                          ...row,
                          field: value,
                          value: row.value,
                          keys: updatedField.options.keys
                        }
                      : {
                          field: value,
                          rowOperator: '=',
                          type: updatedField.options.type,
                          keys: updatedField.options.keys,
                          value:
                            updatedField.options.type === 'timestamp'
                              ? moment.utc().format()
                              : ''
                        };
                  onRowUpdate(idx, rowUpdate);
                }}
              />

              <div className={css.operator}>
                <span
                  className={css.link}
                  onClick={(event) => {
                    setAnchorEl(event.currentTarget);
                    setActiveAnchor(idx);
                  }}
                >
                  {
                    operators[type].find((op) => op.value === rowOperator)
                      ?.label
                  }
                </span>
                <Menu
                  open={!!anchorEl && activeAnchor === idx}
                  anchorEl={anchorEl}
                  classes={{ paper: css.menuPaper }}
                  onClose={() => {
                    setAnchorEl(null);
                    setActiveAnchor(undefined);
                  }}
                >
                  {operators[type].map((op) => (
                    <MenuItem
                      key={`${type}-${op.label}`}
                      onClick={() => {
                        onRowUpdate(idx, { ...row, rowOperator: op.value });
                        setAnchorEl(null);
                        setActiveAnchor(undefined);
                      }}
                    >
                      {op.label}
                    </MenuItem>
                  ))}
                </Menu>
              </div>

              {type === 'timestamp' ? (
                <MuiPickersUtilsProvider utils={MomentUtils}>
                  <DateTimePicker
                    variant="inline"
                    inputVariant="outlined"
                    size="small"
                    value={moment.utc(value)}
                    onChange={(date) => {
                      onRowUpdate(idx, {
                        ...row,
                        value: date ? date.format() : ''
                      });
                    }}
                    format="MM/DD/YYYY hh:mm:ss a"
                    InputProps={{
                      endAdornment: (
                        <Typography variant="caption" color="textSecondary">
                          UTC
                        </Typography>
                      )
                    }}
                    fullWidth
                  />
                </MuiPickersUtilsProvider>
              ) : (
                <TextField
                  className={css.rowValue}
                  variant="outlined"
                  size="small"
                  value={value}
                  type={type === 'int' ? 'number' : 'text'}
                  error={showErrors && isInvalidValue}
                  onChange={(event) =>
                    onRowUpdate(idx, { ...row, value: event.target.value })
                  }
                />
              )}

              {rowData.length > 1 ? (
                <CloseIcon
                  fontSize="inherit"
                  className={css.removeRowIcon}
                  onClick={() => onRowUpdate(idx)}
                />
              ) : null}
            </div>
          </Fragment>
        );
      })}

      <span
        className={css.link}
        onClick={(event) => {
          if (operator) {
            onNewRow();
          } else {
            setOperatorEl(event.currentTarget);
          }
        }}
      >
        + Add Row
      </span>
      <Menu
        open={!!operatorEl}
        anchorEl={operatorEl}
        classes={{ paper: css.menuPaper }}
        onClose={() => setOperatorEl(null)}
      >
        {groupOperators.map((op: Operator) => (
          <MenuItem
            key={op}
            onClick={() => {
              onNewRow(op);
              setOperatorEl(null);
            }}
          >
            {op}
          </MenuItem>
        ))}
      </Menu>
    </div>
  );
};
