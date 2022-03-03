import React, { useEffect, useState } from 'react';
import { MoleculaTable } from './MoleculaTable';
import { MoleculaTables } from './MoleculaTables';
import { pilosa } from 'services/eventServices';
import { useEffectOnce } from 'react-use';
import { useRouteMatch } from 'react-router-dom';
import { useHistory } from 'react-router-dom';

export const MoleculaTablesContainer = () => {
  const match = useRouteMatch('/tables/:table');
  const history = useHistory();
  const [tables, setTables] = useState<any>();
  const [selectedTable, setSelectedTable] = useState<any>();
  const [maxSize] = useState<number>(0);
  const [lastUpdated] = useState<string>('');

  useEffectOnce(() => {
    pilosa.get
      .schema()
      .then((res) => setTables(res.data.indexes))
      .finally(() =>
        pilosa.get
          .schemaDetails()
          .then((res) => setTables(res.data.indexes))
          .catch((err) => console.log(err))
      );
  });

  useEffect(() => {
    if (match && tables) {
      const tableName = match?.params['table'];
      const matchTable = tables.find((t) => t.name === tableName);
      if (matchTable) {
        setSelectedTable(matchTable);
      } else {
        history.push('/tables');
      }
    } else {
      setSelectedTable(undefined);
    }
  }, [match, tables, history]);

  return selectedTable ? (
    <MoleculaTable table={selectedTable} lastUpdated={lastUpdated} />
  ) : (
    <MoleculaTables
      tables={tables}
      lastUpdated={lastUpdated}
      maxSize={maxSize}
    />
  );
};
