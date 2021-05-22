import React, { FC } from 'react';
import FormControl from '@material-ui/core/FormControl';
import InputLabel from '@material-ui/core/InputLabel';
import MenuItem from '@material-ui/core/MenuItem';
import {
  Select as MuiSelect,
  SelectProps as MuiSelectProps
} from '@material-ui/core';
import css from './Select.module.scss';

export type OptionType = {
  label: string;
  value: string;
  disabled?: boolean;
};

type SelectProps = {
  options: OptionType[];
  label?: string;
  allowEmpty?: boolean;
  onChange: (value: any) => void;
} & MuiSelectProps;

export const Select: FC<SelectProps> = ({
  defaultValue,
  options,
  label = '',
  allowEmpty = false,
  fullWidth = true,
  onChange,
  className,
  ...rest
}) => (
  <FormControl
    className={className}
    variant="outlined"
    size="small"
    fullWidth={fullWidth}
  >
    <InputLabel>{label}</InputLabel>
    <MuiSelect
      label={label}
      defaultValue={defaultValue ? defaultValue : ''}
      onChange={(event) => onChange(event.target.value)}
      MenuProps={{
        classes: {
          paper: css.menuPaper
        }
      }}
      {...rest}
    >
      {allowEmpty ? <MenuItem value="">&nbsp;</MenuItem> : null}
      {options.map((option) => (
        <MenuItem
          key={option.value}
          value={option.value}
          disabled={option.disabled}
          dense
        >
          {option.label}
        </MenuItem>
      ))}
    </MuiSelect>
  </FormControl>
);
