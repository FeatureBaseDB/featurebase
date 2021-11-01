import React, { FC, Fragment, useEffect, useRef, useState } from 'react';
import ArrowDropDownIcon from '@material-ui/icons/ArrowDropDown';
import classNames from 'classnames';
import OrderBy from 'lodash/orderBy';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
// import TablePagination from '@material-ui/core/TablePagination';
import TableRow from '@material-ui/core/TableRow';
import Typography from '@material-ui/core/Typography';
import { ColumnInfo } from 'proto/pilosa_pb';
import { Pager } from 'shared/Pager';
import { formatTableCell } from 'shared/utils/formatTableCell';
import css from './DataTable.module.scss';

type TableProps = {
  headers: ColumnInfo.AsObject[];
  data: any[];
  loading?: boolean;
  autoWidth?: boolean;
};

export const DataTable: FC<TableProps> = ({
  headers,
  data,
  loading = false,
  autoWidth = false
}) => {
  const [sortedData, setSortedData] = useState<any[]>(data);
  const [sort, setSort] = useState<string>(headers[0]?.name);
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc');
  const [page, setPage] = useState<number>(1);
  const [rowsPerPage, setRowsPerPage] = useState<number>(10);
  const resultsRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    setSortedData(OrderBy(data, [sort], [sortDir]));
    setPage(1);
  }, [data, sort, sortDir]);

  const onChangePage = (newPage: number) => {
    setPage(newPage);
    scrollToResultsTop();
  };

  const onChangePerPage = (perPage: number) => {
    setPage(1);
    setRowsPerPage(perPage);
    scrollToResultsTop();
  };

  const scrollToResultsTop = () => {
    setTimeout(() => {
      if (resultsRef.current) {
        resultsRef.current.scrollIntoView({
          behavior: 'smooth',
          block: 'start'
        });
      }
    }, 0);
  };

  const onSortClick = (name: string) => {
    if (sort === name) {
      setSortDir(sortDir === 'desc' ? 'asc' : 'desc');
    } else {
      setSort(name);
      setSortDir('asc');
    }
  };



  return (
    <Fragment>
      <div ref={resultsRef} />
      <div className={css.tableWrapper}>
        <Table>
          <TableHead>
            <TableRow>
              {headers.map((col) => (
                <TableCell
                  key={`table-header-${col.name}`}
                  className={css.tableHeader}
                >
                  <span
                    className={classNames(css.sortable, {
                      [css.currentSort]: sort === col.name
                    })}
                    onClick={() => onSortClick(col.name)}
                  >
                    {col.name}
                    <ArrowDropDownIcon
                      className={classNames(css.sortArrow, {
                        [css.asc]: sortDir === 'asc'
                      })}
                    />
                  </span>
                </TableCell>
              ))}
              {autoWidth ? <TableCell className={css.fillWidth} /> : null}
            </TableRow>
          </TableHead>
          <TableBody className={css.tableBody}>
            {!loading &&
              sortedData
                .slice(
                  (page - 1) * rowsPerPage,
                  (page - 1) * rowsPerPage + rowsPerPage
                )
                .map((row, rowIdx) => (
                  <TableRow
                    key={`table-row-${rowIdx}`}
                    className={rowIdx % 2 === 0 ? '' : css.altBg}
                  >
                    {headers.map((col, colIdx) => (
                      <TableCell
                        key={`table-cell-${rowIdx}-${colIdx}`}
                        className={css.tableCell}
                      >
                        {formatTableCell(row, col)}
                            </TableCell>
                    ))}
                    {autoWidth ? <TableCell className={css.fillWidth} /> : null}
                  </TableRow>
                ))}

            {!loading && sortedData.length === 0 && (
              <TableRow>
                <TableCell colSpan={headers.length}>
                  <Typography variant="caption" color="textSecondary">
                    No Results
                  </Typography>
                </TableCell>
              </TableRow>
            )}

            {loading && (
              <TableRow>
                <TableCell colSpan={headers.length}>
                  <Typography variant="caption" color="textSecondary">
                    Loading...
                  </Typography>
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>

      <Pager
        className={css.pagination}
        page={page}
        rowsPerPage={rowsPerPage}
        totalItems={sortedData.length}
        showTotal={true}
        onChangePage={onChangePage}
        onChangePerPage={onChangePerPage}
      />
    </Fragment>
  );
};
