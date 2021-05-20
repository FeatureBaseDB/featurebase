import React, { FC } from 'react';
import FormControl from '@material-ui/core/FormControl';
import InputLabel from '@material-ui/core/InputLabel';
import MenuItem from '@material-ui/core/MenuItem';
import Select from '@material-ui/core/Select';
import css from './SortBy.module.scss';

type SortByProps = {
  options: { label: string; value: string }[];
  defaultValue: string;
  onChange: (value: any) => void;
  sortId: string;
};

export const SortBy: FC<SortByProps> = ({
  options,
  defaultValue,
  onChange,
  sortId
}) => {
  return (
    <FormControl variant="outlined" size="small">
      <InputLabel id={`${sortId}-label`}>Sort By</InputLabel>
      <Select
        labelId={`${sortId}-label`}
        id={sortId}
        defaultValue={defaultValue}
        onChange={(event) => onChange(event.target.value)}
        label="Sort By"
        MenuProps={{
          classes: {
            paper: css.menuPaper
          }
        }}
      >
        {options.map((option) => (
          <MenuItem key={option.value} value={option.value} dense>
            {option.label}
          </MenuItem>
        ))}
      </Select>
    </FormControl>
  );
};
