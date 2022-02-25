import React, { FC, Fragment, useCallback, useEffect, useState } from 'react';
import CollapseAllIcon from '@material-ui/icons/UnfoldLess';
import ExpandAllIcon from '@material-ui/icons/UnfoldMore';
import IconButton from '@material-ui/core/IconButton';
import Paper from '@material-ui/core/Paper';
import Tooltip from '@material-ui/core/Tooltip';
import Typography from '@material-ui/core/Typography';
import { Block } from 'shared/Block';
import { CLUSTER_STATUS } from './clusterStatus';
import { ImportStatus } from './ImportStatus';
import { Metrics } from './Metrics';
import { Node } from './Node';
import { pilosa } from 'services/eventServices';
import { StatusIndicator } from 'shared/StatusIndicator';
import { useEffectOnce } from 'react-use';
import css from './ClusterHealth.module.scss';

export const ClusterHealth: FC = () => {
  const [cluster, setCluster] = useState<any>();
  const [metrics, setMetrics] = useState<any>();
  const [info, setInfo] = useState<any>();
  const [expanded, setExpanded] = useState<string[]>([]);
  const [showMetrics, setShowMetrics] = useState<any>();
  const allExpanded = cluster && expanded.length === cluster.nodes.length;

  useEffectOnce(() => {
    getClusterHealth();
  });

  const refreshMetrics = useCallback(() => {
    pilosa.get
      .metrics()
      .then((res) => setMetrics(res.data))
      .catch(() => setMetrics(undefined));
  }, []);

  useEffect(() => {
    const interval = setInterval(() => {
      getClusterHealth();
      refreshMetrics();
    }, 15000);
    return () => clearInterval(interval);
  }, [refreshMetrics, cluster]);

  const getClusterHealth = () => {
    pilosa.get
      .status()
      .then((res) => {
        setCluster(res.data);
      })
      .catch(() => setCluster(undefined));

    let clusterInfo = {};
    pilosa.get
      .info()
      .then((res) => {
        clusterInfo = { ...res.data };
      })
      .then(() => {
        pilosa.get.version().then((res) => {
          clusterInfo = { ...clusterInfo, ...res.data };
          setInfo(clusterInfo);
        });
      })
      .catch(() => setInfo(undefined));

    pilosa.get
      .metrics()
      .then((res) => setMetrics(res.data))
      .catch(() => setMetrics(undefined));
  };

  const toggleAccordion = (nodeId: string) => {
    const isExpanded = expanded.includes(nodeId);
    if (isExpanded) {
      const newExpanded = expanded.filter((n) => n !== nodeId);
      setExpanded(newExpanded);
    } else {
      setExpanded([...expanded, nodeId]);
    }
  };

  const expandAllNodes = () => {
    const allNodeIds = cluster.nodes.map((n) => n.id);
    setExpanded(allNodeIds);
  };

  return (
    <Fragment>
      <Block>
        <div className={css.header}>
          {cluster ? (
            <StatusIndicator
              state={cluster.state}
              stateDetails={CLUSTER_STATUS[cluster.state]}
              margin="both"
            />
          ) : null}
          <Typography variant="h5" color="textSecondary">
            Cluster Health
          </Typography>
          {cluster && cluster.nodes.length > 1 && (
            <div className={css.expandAllIcon}>
              <Tooltip
                title={allExpanded ? 'Collapse all nodes' : 'Expand all nodes'}
                placement="left"
                arrow
              >
                {allExpanded ? (
                  <IconButton size="small" onClick={() => setExpanded([])}>
                    <CollapseAllIcon aria-label="Collapse all nodes" />
                  </IconButton>
                ) : (
                  <IconButton size="small" onClick={expandAllNodes}>
                    <ExpandAllIcon aria-label="Expand all nodes" />
                  </IconButton>
                )}
              </Tooltip>
            </div>
          )}
        </div>

        {cluster && info ? (
          <div className={css.nodes}>
            {cluster.nodes.map((node) => (
              <Node
                key={node.id}
                node={node}
                info={info}
                expanded={expanded.includes(node.id)}
                onToggle={() => toggleAccordion(node.id)}
                onMetricClick={() => setShowMetrics(node)}
              />
            ))}
          </div>
        ) : (
          <Paper className={css.pilosaError}>
            <Typography variant="caption" color="textSecondary">
              There is a problem connecting to Pilosa.
            </Typography>
          </Paper>
        )}

        {metrics ? (
          <div className={css.activity}>
            <ImportStatus metrics={metrics} />
          </div>
        ) : null}
      </Block>
      {showMetrics && (
        <Metrics
          open={!!showMetrics}
          onClose={() => setShowMetrics(undefined)}
          node={showMetrics}
        />
      )}
    </Fragment>
  );
};
