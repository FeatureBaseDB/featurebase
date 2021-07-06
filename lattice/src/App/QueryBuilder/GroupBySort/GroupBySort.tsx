import React, { FC } from 'react';
import { Select } from 'shared/Select';
import css from './GroupBySort.module.scss';

export type SortOption = {
  sortValue: string;
  field?: string;
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
  const hasPrimary = sort.length > 0;
  const primary = hasPrimary ? sort[0].sortValue.split(' ')[0] : '';
  const hasSecondary = sort.length > 1;
  const secondary = hasSecondary ? sort[1].sortValue.split(' ')[0] : '';
  const sortOptions = [
    { label: 'Count (desc)', value: 'count desc' },
    { label: 'Count (asc)', value: 'count asc' },
    { label: 'Sum (desc)', value: 'sum desc' },
    { label: 'Sum (asc)', value: 'sum asc' }
  ];

  const onPrimaryChange = (value: string) => {
    const split = value.split(' ');
    const isSum = split[0] === 'sum';
    const fieldValue = hasPrimary ? sort[0].field : undefined;

    if (!hasSecondary) {
      onUpdate([{ sortValue: value, field: isSum ? fieldValue : undefined }]);
    } else if (hasSecondary && sort[1].sortValue.includes(split[0])) {
      onUpdate([{ sortValue: value, field: isSum ? fieldValue : undefined }]);
    } else {
      onUpdate([
        { sortValue: value, field: isSum ? fieldValue : undefined },
        sort[1]
      ]);
    }
  };
  const onSecondaryChange = (value: string) => {
    let sortOp = { sortValue: value };

    if (hasSecondary) {
      sortOp = { ...sort[1], ...sortOp };
    }
    onUpdate([sort[0], sortOp]);
  };

  const onUpdateSumField = (isPrimary: boolean, value: string) => {
    let clone = [...sort];

    if (isPrimary) {
      clone[0].field = value;
    } else {
      clone[1].field = value;
    }

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
        />

        {primary.includes('sum') ? (
          <Select
            className={css.fieldSelect}
            label="Field"
            value={sort[0].field ? sort[0].field : ''}
            options={fields}
            onChange={(value) => onUpdateSumField(true, value)}
            error={showErrors ? !sort[0].field : false}
          />
        ) : null}
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
          />

          {secondary.includes('sum') ? (
            <Select
              className={css.fieldSelect}
              label="Field"
              value={sort[1].field ? sort[1].field : ''}
              options={fields}
              onChange={(value) => onUpdateSumField(false, value)}
              error={showErrors ? !sort[1].field : false}
            />
          ) : null}
        </div>
      ) : null}
    </div>
  );
};
