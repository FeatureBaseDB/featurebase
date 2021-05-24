import React, { FC } from 'react';
import classNames from 'classnames';
import Pagination from '@material-ui/lab/Pagination';
import Pluralize from 'react-pluralize';
import { Select } from 'shared/Select';
import css from './Pager.module.scss';

type PagerProps = {
  page: number;
  rowsPerPage: number;
  totalItems: number;
  showTotal?: boolean;
  className?: any;
  onChangePage: (page: number) => void;
  onChangePerPage?: (rowsPerPage: number) => void;
};

export const Pager: FC<PagerProps> = ({
  page,
  rowsPerPage,
  totalItems,
  showTotal = true,
  className,
  onChangePage,
  onChangePerPage
}) => {
  const numPages = Math.ceil(totalItems / rowsPerPage);
  const startResults = (page - 1) * rowsPerPage + 1;
  const endResults = Math.min(startResults + rowsPerPage - 1, totalItems);

  return (
    <div className={classNames(css.pager, className)}>
      <div className={css.pages}>
        <Pagination
          size="small"
          count={numPages}
          page={page}
          onChange={(_event, value) => onChangePage(value)}
        />
        {onChangePerPage ? (
          <div className={css.perPageSelector}>
            <Select
              label="Per Page"
              value={rowsPerPage}
              options={[
                { label: '10', value: '10' },
                { label: '25', value: '25' },
                { label: '50', value: '50' }
              ]}
              onChange={(value) => onChangePerPage(Number(value))}
              fullWidth
            />
          </div>
        ) : null}
      </div>
      {showTotal && (
        <div className={css.total}>
          Showing {startResults} - {endResults} of{` `}
          <Pluralize singular="result" count={totalItems} />
        </div>
      )}
    </div>
  );
};
