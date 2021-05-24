import React, { useState } from 'react';
import Paper from '@material-ui/core/Paper';
import reverse from 'lodash/reverse';
import sortBy from 'lodash/sortBy';
import Typography from '@material-ui/core/Typography';
import uniqBy from 'lodash/uniqBy';
import { Block } from 'shared/Block';
import { NodeIndicator } from './NodeIndicator';
import { pilosa } from 'services/eventServices';
import { QueryItem } from './QueryItem';
import { SortBy } from 'shared/SortBy';
import { useEffectOnce } from 'react-use';
import css from './QueryHistory.module.scss';

export const QueryHistory = () => {
  const [queries, setQueries] = useState<any[]>([]);
  const [nodes, setNodes] = useState<string[]>([]);
  const [hideNodes, setHideNodes] = useState<string[]>([]);
  useEffectOnce(() => getQueries());

  const getQueries = () => {
    pilosa.get.queryHistory().then((res) => {
      setQueries(res.data);
      const uniqNodes = uniqBy(Array.from(res.data, (i: any) => i.nodeID));
      setNodes(uniqNodes);
    });
  };

  const handleSortChange = (value: any) => {
    if (value === 'runtime-desc') {
      setQueries(sortBy(queries, 'runtime'));
    } else if (value === 'runtime-asc') {
      setQueries(reverse(sortBy(queries, 'runtime')));
    } else {
      setQueries(sortBy(queries, 'start'));
    }
  };

  const handleNodeFilterClick = (node: string) => {
    if (hideNodes.includes(node)) {
      const newNodes = hideNodes.filter((n) => n !== node);
      setHideNodes(newNodes);
    } else {
      setHideNodes([...hideNodes, node]);
    }
  };

  return (
    <Block>
      <div className={css.header}>
        <Typography variant="h5" color="textSecondary">
          Recent Query History
        </Typography>
      </div>
      <div className={css.actions}>
        <SortBy
          options={[
            { label: 'Most Recent', value: 'starttime' },
            { label: 'Slowest Runtime', value: 'runtime-asc' },
            { label: 'Fastest Runtime', value: 'runtime-desc' }
          ]}
          defaultValue="starttime"
          onChange={handleSortChange}
          sortId="sort-by"
        />

        {nodes.length > 1 ? (
          <div className={css.nodeFilter}>
            <label className={css.filterLabel}>Filter by Node</label>
            <div className={css.indicators}>
              {nodes.map((node, idx) => (
                <NodeIndicator
                  key={`node-ind-${node}`}
                  node={node}
                  nodeIdx={idx}
                  tooltip={true}
                  off={hideNodes.includes(node)}
                  onClick={() => handleNodeFilterClick(node)}
                />
              ))}
            </div>
          </div>
        ) : null}
      </div>
      <div>
        {queries
          .filter((q) => !hideNodes.includes(q.nodeID))
          .map((query) => (
            <QueryItem
              key={query.start}
              item={query}
              nodeIdx={nodes.indexOf(query.nodeID)}
            />
          ))}

        {queries.filter((q) => !hideNodes.includes(q.nodeID)).length === 0 ? (
          <Paper className={css.noQueries}>
            <Typography variant="caption" color="textSecondary">
              No recent queries.
            </Typography>
          </Paper>
        ) : null}
      </div>
    </Block>
  );
};
