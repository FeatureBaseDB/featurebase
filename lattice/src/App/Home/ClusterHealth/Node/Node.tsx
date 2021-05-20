import React, { FC, Fragment, useState } from 'react';
import Button from '@material-ui/core/Button';
import copy from 'copy-to-clipboard';
import EqualizerIcon from '@material-ui/icons/EqualizerSharp';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import Accordion from '@material-ui/core/Accordion';
import AccordionSummary from '@material-ui/core/AccordionSummary';
import AccordionDetails from '@material-ui/core/AccordionDetails';
import FileCopySharpIcon from '@material-ui/icons/FileCopySharp';
import Find from 'lodash/find';
import IconButton from '@material-ui/core/IconButton';
import InfoIcon from '@material-ui/icons/Info';
import Tooltip from '@material-ui/core/Tooltip';
import Typography from '@material-ui/core/Typography';
import { formatBytes } from 'shared/utils/formatBytes';
import { nodeInfo } from './nodeInfo';
import { NODE_STATE } from './nodeStatus';
import { StatusIndicator } from 'shared/StatusIndicator';
import css from './Node.module.scss';

type NodeType = {
  node: any;
  info: any;
  usage: any;
  expanded: boolean;
  onToggle: () => void;
  onMetricClick: () => void;
};

export const Node: FC<NodeType> = ({
  node,
  info,
  usage,
  expanded,
  onToggle,
  onMetricClick
}) => {
  const [copyHost, setCopyHost] = useState<string>('Copy Host');
  const [copyID, setCopyID] = useState<string>('Click to Copy');
  const { id, isPrimary, state } = node;
  const diskTotalInUse = usage?.diskUsage?.totalInUse;
  const diskCapacity = usage?.diskUsage?.capacity;
  const diskUsagePercentage = diskCapacity
    ? (diskTotalInUse / diskCapacity) * 100
    : undefined;
  const memoryTotalInUse = usage?.memoryUsage?.totalInUse;
  const memoryCapacity = usage?.memoryUsage?.capacity;
  const memoryUsagePercentage = memoryCapacity
    ? (memoryTotalInUse / memoryCapacity) * 100
    : undefined;
  const keys = Object.keys(info);

  const onCopyHostClick = () => {
    copy(`${node.uri.host}:${node.uri.port}`);
    setCopyHost('Copied!');
    setTimeout(() => {
      setCopyHost('Copy Host');
    }, 1500);
  };

  const onCopyIdClick = () => {
    copy(id);
    setCopyID('Copied!');
    setTimeout(() => {
      setCopyID('Click to Copy');
    }, 1500);
  };

  return (
    <Accordion expanded={expanded} onChange={onToggle}>
      <AccordionSummary expandIcon={<ExpandMoreIcon />}>
        <div className={css.header}>
          <StatusIndicator
            state={state}
            stateDetails={NODE_STATE[state]}
            margin="right"
            size="small"
          />
          <code>{`${node.uri.host}:${node.uri.port}`}</code>
          <Tooltip title={copyHost} placement="top" arrow>
            <IconButton
              className={css.copyIcon}
              size="small"
              color="inherit"
              onClick={(e) => {
                e.stopPropagation();
                onCopyHostClick();
              }}
            >
              <FileCopySharpIcon fontSize="inherit" />
            </IconButton>
          </Tooltip>
        </div>
        {isPrimary ? <span>(Primary)</span> : null}
      </AccordionSummary>
      <AccordionDetails className={css.details}>
        <div className={css.node}>
          <div className={css.nodeId}>
            <span className={css.label}>Node Id:</span>
            <Tooltip title={copyID} placement="top" arrow>
              <span className={css.value} onClick={onCopyIdClick}>
                {node.id}
              </span>
            </Tooltip>
          </div>
          <div className={css.nodeUsage}>
            <div>
              <div className={css.label}>Disk Usage:</div>
              <div>
                {usage ? (
                  <Fragment>
                    <Typography variant="caption">
                      {formatBytes(diskTotalInUse)}
                      {diskCapacity
                        ? ` used out of ${formatBytes(diskCapacity)}`
                        : null}
                    </Typography>
                    <div className={css.totalCapacity}>
                      {diskUsagePercentage ? (
                        <Tooltip
                          title={
                            <Typography variant="caption">
                              {diskUsagePercentage < 1
                                ? '< 1'
                                : diskUsagePercentage.toLocaleString(
                                    undefined,
                                    { maximumFractionDigits: 1 }
                                  )}
                              % used
                            </Typography>
                          }
                          placement="top"
                          arrow
                        >
                          <div
                            className={css.totalInUse}
                            style={{
                              width: `${
                                diskUsagePercentage < 1
                                  ? 1
                                  : diskUsagePercentage
                              }%`
                            }}
                          />
                        </Tooltip>
                      ) : (
                        <Fragment>
                          <Tooltip
                            title={
                              <Typography variant="caption">
                                {formatBytes(diskTotalInUse)} used
                              </Typography>
                            }
                            placement="top"
                            arrow
                          >
                            <div
                              className={css.totalInUse}
                              style={{ width: '2%' }}
                            />
                          </Tooltip>
                          <Typography
                            className={css.unknownCapacity}
                            variant="caption"
                            color="textSecondary"
                          >
                            Node disk capacity unknown
                          </Typography>
                        </Fragment>
                      )}
                    </div>
                  </Fragment>
                ) : (
                  <Typography variant="caption" paragraph>
                    Calculating...
                  </Typography>
                )}
              </div>
            </div>
            <div>
              <div className={css.label}>Memory Usage:</div>
              <div>
                {usage ? (
                  <Fragment>
                    <Typography variant="caption">
                      {formatBytes(memoryTotalInUse)}
                      {memoryCapacity
                        ? ` used out of ${formatBytes(memoryCapacity)}`
                        : null}
                    </Typography>
                    <div className={css.totalCapacity}>
                      {memoryUsagePercentage ? (
                        <Tooltip
                          title={
                            <Typography variant="caption">
                              {memoryUsagePercentage < 1
                                ? '< 1'
                                : memoryUsagePercentage.toLocaleString(
                                    undefined,
                                    { maximumFractionDigits: 1 }
                                  )}
                              % used
                            </Typography>
                          }
                          placement="top"
                          arrow
                        >
                          <div
                            className={css.totalInUse}
                            style={{
                              width: `${
                                memoryUsagePercentage < 1
                                  ? 1
                                  : memoryUsagePercentage
                              }%`
                            }}
                          />
                        </Tooltip>
                      ) : (
                        <Fragment>
                          <Tooltip
                            title={
                              <Typography variant="caption">
                                {formatBytes(memoryTotalInUse)} used
                              </Typography>
                            }
                            placement="top"
                            arrow
                          >
                            <div
                              className={css.totalInUse}
                              style={{ width: '2%' }}
                            />
                          </Tooltip>
                          <Typography
                            className={css.unknownCapacity}
                            variant="caption"
                            color="textSecondary"
                          >
                            Node memory capacity unknown
                          </Typography>
                        </Fragment>
                      )}
                    </div>
                  </Fragment>
                ) : (
                  <Typography variant="caption" paragraph>
                    Calculating...
                  </Typography>
                )}
              </div>
            </div>
          </div>
          <div className={css.nodeSettings}>
            {keys.map((key) => {
              const showNode = Find(nodeInfo, (node) => node.name === key);
              if (showNode) {
                return (
                  <div key={key} className={css.cell}>
                    <label>
                      <span className={css.nodeInfoLabel}>{key}</span>
                      {showNode.tooltip ? (
                        <Tooltip title={showNode.tooltip} placement="top" arrow>
                          <InfoIcon fontSize="inherit" />
                        </Tooltip>
                      ) : null}
                    </label>
                    <div>
                      {key === 'memory'
                        ? formatBytes(info[key])
                        : info[key].toLocaleString()}
                    </div>
                  </div>
                );
              }
              return null;
            })}
          </div>
          <div className={css.tableHeader}>
            <div className={css.key} />
            <div className={css.scheme}>Scheme</div>
            <div className={css.host}>Host</div>
            <div className={css.port}>Port</div>
          </div>
          {node.uri && (
            <div className={css.tableRow}>
              <div className={css.key}>
                <span>uri</span>
              </div>
              <div className={css.scheme}>{node.uri.scheme}</div>
              <div className={css.host}>{node.uri.host}</div>
              <div className={css.port}>{node.uri.port}</div>
            </div>
          )}
          {node['grpc-uri'] && (
            <div className={css.tableRow}>
              <div className={css.key}>
                <span>grpc-uri</span>
              </div>
              <div className={css.scheme}>{node['grpc-uri'].scheme}</div>
              <div className={css.host}>{node['grpc-uri'].host}</div>
              <div className={css.port}>{node['grpc-uri'].port}</div>
            </div>
          )}

          <div className={css.metricsLink}>
            <Button
              size="small"
              variant="contained"
              onClick={onMetricClick}
              startIcon={<EqualizerIcon />}
            >
              View Metrics
            </Button>
          </div>
        </div>
      </AccordionDetails>
    </Accordion>
  );
};
