import React, { FC } from 'react';
import { Select } from 'shared/Select';
import css from './GroupBySort.module.scss';

export type SortOption = {
  sortValue: string;
  sum?: string;
  aggregate?: string;
};

type GroupBySortProps = {
  sort: SortOption[];
  fields: { label: string; value: string }[];
  showErrors: boolean;
  onUpdate: (sort: SortOption[]) => void;
};

export const GroupBySort: FC<GroupBySortProps> = ({
  sort,
  fields,
  showErrors,
  onUpdate
}) => {
  const hasPrimary = sort.length > 0 && sort[0].sortValue;
  const primary = hasPrimary ? sort[0].sortValue.split(' ')[0] : '';
  const hasSecondary = sort.length > 1;
  const secondary = hasSecondary ? sort[1].sortValue.split(' ')[0] : '';
  const sortOptions = [
    { label: 'Count (desc)', value: 'count desc' },
    { label: 'Count (asc)', value: 'count asc' },
    { label: 'Sum (desc)', value: 'sum desc' },
    { label: 'Sum (asc)', value: 'sum asc' },
    { label: 'Aggregate (desc)', value: 'aggregate desc' },
    { label: 'Aggregate (asc)', value: 'aggregate asc' }
  ];

  const onPrimaryChange = (value: string) => {
    const split = value.split(' ');

    if (!hasSecondary) {
      onUpdate([{ sortValue: value }]);
    } else if (sort[1].sortValue.includes(split[0])) {
      onUpdate([{ sortValue: value }]);
    } else {
      onUpdate([{ sortValue: value }, sort[1]]);
    }
  };

  const onSecondaryChange = (value: string) => {
    let sortOp = { sortValue: value };

    if (hasSecondary) {
      sortOp = { ...sort[1], ...sortOp };
    }
    onUpdate([sort[0], sortOp]);
  };

  const renderFieldSelect = (
    sortFor: 'primary' | 'secondary',
    sortType: 'sum' | 'aggregate'
  ) => {
    const sortIdx = sortFor === 'primary' ? 0 : 1;

    return (
      <Select
        className={css.fieldSelect}
        label="Field"
        value={sort[sortIdx][sortType] ? sort[sortIdx][sortType] : ''}
        options={fields}
        onChange={(value) => onUpdateField(sortType, sortIdx, value)}
        error={showErrors ? !sort[sortIdx][sortType] : false}
      />
    );
  };

  const onUpdateField = (
    sortType: 'sum' | 'aggregate',
    sortIdx: number,
    value: string
  ) => {
    let clone = [...sort];
    clone[sortIdx][sortType] = value;
    onUpdate(clone);
  };

  return (
    <div>
      <div className={css.sortSelectRow}>
        <Select
          label="Primary Sort"
          value={hasPrimary ? sort[0].sortValue : ''}
          options={sortOptions}
          onChange={(value) => onPrimaryChange(value)}
          allowEmpty={true}
        />

        {primary.includes('sum')
          ? renderFieldSelect('primary', 'sum')
          : primary.includes('aggregate')
          ? renderFieldSelect('primary', 'aggregate')
          : null}
      </div>

      {hasPrimary ? (
        <div className={css.sortSelectRow}>
          <Select
            label="Secondary Sort"
            value={hasSecondary ? sort[1].sortValue : ''}
            options={sortOptions.filter(
              (option) => !option.value.includes(primary)
            )}
            onChange={(value) => onSecondaryChange(value)}
            allowEmpty={true}
          />

          {secondary.includes('sum')
            ? renderFieldSelect('secondary', 'sum')
            : secondary.includes('aggregate')
            ? renderFieldSelect('secondary', 'aggregate')
            : null}
        </div>
      ) : null}
    </div>
  );
};
