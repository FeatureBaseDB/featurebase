import React, { useEffect, useState } from 'react';
import OrderBy from 'lodash/orderBy';
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
  const [dataDistribution, setDataDistribution] = useState<any>();
  const [maxSize, setMaxSize] = useState<number>(0);

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
      
    pilosa.get.usage().then((res) => {
      const nodes = Object.keys(res.data);
      let data = {};
      nodes.forEach((node) => {
        const nodeIndexes = res.data[node].diskUsage.indexes;
        const indexList = Object.keys(nodeIndexes);
        indexList.forEach((i) => {
          const nodeData = nodeIndexes[i];
          if (data[i]) {
            data[i] = {
              total: data[i].total + nodeData.total,
              fieldKeysTotal: data[i].fieldKeysTotal + nodeData.fieldKeysTotal,
              indexKeys: data[i].indexKeys + nodeData.indexKeys,
              fragments: data[i].fragments + nodeData.fragments,
              metadata: data[i].metadata + nodeData.metadata,
              fields: [...data[i].fields, nodeData.fields]
            };
          } else {
            data[i] = {
              total: nodeData.total,
              fieldKeysTotal: nodeData.fieldKeysTotal,
              indexKeys: nodeData.indexKeys,
              fragments: nodeData.fragments,
              metadata: nodeData.metadata,
              fields: [nodeData.fields]
            };
          }
        });
      });

      const sorted = OrderBy(data, ['total'], ['desc']);
      if (sorted.length > 0) {
        setMaxSize(sorted[0].total);
      }

      setDataDistribution(data);
    });
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
    <MoleculaTable
      table={selectedTable}
      dataDistribution={
        dataDistribution ? dataDistribution[selectedTable.name] : undefined
      }
    />
  ) : (
    <MoleculaTables
      tables={tables}
      dataDistribution={dataDistribution}
      maxSize={maxSize}
    />
  );
};
