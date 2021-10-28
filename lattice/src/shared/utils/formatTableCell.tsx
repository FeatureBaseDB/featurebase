import moment from 'moment';
export const formatTableCell = (row: any, col: any, css: any) => {
     if (typeof row[col.name] === 'object') {
        return (
      <pre className={css.preFormat}>
        {JSON.stringify(row[col.name], null, 2)}
      </pre> )
     } else if (row[col.name] !== undefined) {
        if (col.datatype === '[]string') {
            return (
                 <span>
                    {'"' + (row[col.name]) + '"'}
                </span> 
            )
        }
        return (
      <span>
        {col.datatype === 'timestamp' && row[col.name]
          ? moment
              .utc(row[col.name])
              .format('MM/DD/YYYY hh:mm:ss a')
          : row[col.name].toLocaleString()}
      </span> )
    }
     return null
}
