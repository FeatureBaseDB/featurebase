import React, { FC, Fragment, useEffect, useState } from 'react';
import Checkbox from '@material-ui/core/Checkbox';
import Chip from '@material-ui/core/Chip';
import Dialog from '@material-ui/core/Dialog';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import FormControl from '@material-ui/core/FormControl';
import FormControlLabel from '@material-ui/core/FormControlLabel';
import FormGroup from '@material-ui/core/FormGroup';
import Fuse from 'fuse.js';
import Highlighter from 'react-highlight-words';
import IconButton from '@material-ui/core/IconButton';
import Link from '@material-ui/core/Link';
import Pluralize from 'react-pluralize';
import RefreshIcon from '@material-ui/icons/Refresh';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableContainer from '@material-ui/core/TableContainer';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import TextField from '@material-ui/core/TextField';
import Tooltip from '@material-ui/core/Tooltip';
import { pilosa } from 'services/eventServices';
import { priorityMetrics } from './priorityMetrics';
import { Typography } from '@material-ui/core';
import { useEffectOnce } from 'react-use';
import css from './Metrics.module.scss';

type MetricsProps = {
  open: boolean;
  node: any;
  onClose: () => void;
};

export const Metrics: FC<MetricsProps> = ({ open, node, onClose }) => {
  const [loading, setLoading] = useState<boolean>(false);
  const [searchText, setSearchText] = useState<string>('');
  const [showTypes, setShowTypes] = useState<string[]>([
    'gauge',
    'counter',
    'summary'
  ]);
  const [metrics, setMetrics] = useState<any>();
  const [filteredMetrics, setFilteredMetrics] = useState<any>([]);

  useEffectOnce(() => getMetrics());

  useEffect(() => {
    if (metrics) {
      let filteredResults = metrics;
      if (searchText.length > 1) {
        const fuse = new Fuse(metrics, {
          keys: ['name', 'help'],
          minMatchCharLength: 2,
          ignoreLocation: true,
          threshold: 0
        });

        const result = fuse.search(searchText);
        filteredResults = [];
        result.forEach((r: any) => {
          filteredResults.push(r?.item);
        });
      }

      const typeFiltered = filteredResults.filter((node) =>
        showTypes.includes(node.type.toLowerCase())
      );

      setFilteredMetrics(typeFiltered);
    }
  }, [showTypes, searchText, metrics]);

  const getMetrics = () => {
    pilosa.get
      .metrics()
      .then((res) => setMetrics(res.data[node.id]))
      .catch(() => setMetrics(undefined));

    setTimeout(() => {
      setLoading(false);
    }, 500);
  };

  const handleTypeChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const name = event.target.name;
    if (showTypes.includes(name)) {
      const newShowTypes = showTypes.filter((type) => type !== name);
      setShowTypes(newShowTypes);
    } else {
      setShowTypes([...showTypes, name]);
    }
  };

  const renderValue = (value: any) => {
    if (typeof value === 'object') {
      return Object.keys(value).length > 1 ? (
        <pre>{JSON.stringify(value, null, 2)}</pre>
      ) : (
        <code>{JSON.stringify(value, null, 2)}</code>
      );
    } else if (!isNaN(Number(value))) {
      return <code>{Number(value).toLocaleString()}</code>;
    }

    return <code>value</code>;
  };

  const renderFilter = () => (
    <FormControl component="div" className={css.typeFilter}>
      <FormGroup row>
        <FormControlLabel
          control={
            <Checkbox
              checked={showTypes.includes('gauge')}
              onChange={handleTypeChange}
              name="gauge"
              size="small"
            />
          }
          label={<Typography variant="caption">Gauge</Typography>}
        />
        <FormControlLabel
          control={
            <Checkbox
              checked={showTypes.includes('counter')}
              onChange={handleTypeChange}
              name="counter"
              size="small"
            />
          }
          label={<Typography variant="caption">Counter</Typography>}
        />
        <FormControlLabel
          control={
            <Checkbox
              checked={showTypes.includes('summary')}
              onChange={handleTypeChange}
              name="summary"
              size="small"
            />
          }
          label={<Typography variant="caption">Summary</Typography>}
        />
      </FormGroup>
    </FormControl>
  );

  const renderMetric = (item: any) => {
    const metricsKeys = Object.keys(item.metrics[0]);

    return (
      <div key={item.name} className={css.metricItem}>
        <Chip
          size="small"
          label={item.type}
          variant="outlined"
          className={css.metricType}
        />
        <div className={css.metricName}>
          <Highlighter
            highlightClassName={css.highlight}
            searchWords={searchText.length > 1 ? [searchText] : []}
            textToHighlight={item.name}
            autoEscape={true}
          />
        </div>
        <Typography variant="caption" color="textSecondary" paragraph>
          <Highlighter
            highlightClassName={css.highlight}
            searchWords={searchText.length > 1 ? [searchText] : []}
            textToHighlight={item.help}
            autoEscape={true}
          />
        </Typography>
        <TableContainer className={css.dataTable}>
          <Table size="small">
            <TableHead>
              <TableRow>
                {metricsKeys.map((key) => (
                  <TableCell key={`header=${key}`}>
                    <Typography variant="overline" color="textSecondary">
                      {key}
                    </Typography>
                  </TableCell>
                ))}
              </TableRow>
            </TableHead>
            <TableBody>
              {item.metrics.map((metric, idx) => (
                <TableRow key={`metric-${idx}`} className={css.tableRow}>
                  {metricsKeys.map((key) => (
                    <TableCell
                      key={`metric-${idx}-${key}`}
                      className={css.tableValue}
                    >
                      {renderValue(metric[key])}
                    </TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </div>
    );
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="md"
      PaperProps={{ className: css.dialogPaper }}
      fullWidth
    >
      <DialogTitle>
        Metrics:{' '}
        <code>
          {node.uri.host}:{node.uri.port}
        </code>
        <Tooltip
          title={loading ? 'Refreshing...' : 'Refresh Metrics'}
          placement="left"
          arrow
        >
          <IconButton
            size="small"
            onClick={() => {
              setLoading(true);
              getMetrics();
            }}
            className={css.refreshMetrics}
          >
            <RefreshIcon aria-label="Refresh Metrics" />
          </IconButton>
        </Tooltip>
      </DialogTitle>
      <DialogContent>
        <div className={css.infoMessage}>
          Molecula provides metrics for use with Prometheus. Instantaneous
          metrics are shown here for convenience; to take full advantage of this
          feature, consider using a Prometheus instance, perhaps with a{' '}
          <Link
            href="https://prometheus.io/docs/visualization/grafana/"
            target="_blank"
          >
            Grafana dashboard
          </Link>
          .
        </div>
        <div>
          <label className={css.metricsLabel}>Filter Metrics</label>
          {searchText.length > 1 && (
            <Typography className={css.clearFilter} variant="caption">
              <span className={css.reset} onClick={() => setSearchText('')}>
                Clear filter
              </span>
            </Typography>
          )}
        </div>
        <div className={css.filters}>
          <TextField
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            placeholder="Search for Metric"
            variant="outlined"
            size="small"
          />
          {renderFilter()}
        </div>
        <div className={css.metricsList}>
          {filteredMetrics.length > 0 && (
            <Fragment>
              <Typography
                variant="caption"
                color="textSecondary"
                component="div"
                className={css.metricsHeader}
                paragraph
              >
                <div>
                  Showing{' '}
                  <Pluralize singular="metric" count={filteredMetrics.length} />
                </div>
                {loading ? (
                  <div className={css.loading}>
                    <RefreshIcon
                      fontSize="inherit"
                      className={css.loadingIcon}
                    />
                    <span>Refreshing...</span>
                  </div>
                ) : null}
              </Typography>
              {filteredMetrics.map((metric) => {
                const isPriority = priorityMetrics.includes(metric.name);
                if (isPriority) {
                  return renderMetric(metric);
                } else {
                  return null;
                }
              })}
              {filteredMetrics.map((metric) => {
                const isPriority = priorityMetrics.includes(metric.name);
                if (!isPriority) {
                  return renderMetric(metric);
                } else {
                  return null;
                }
              })}
            </Fragment>
          )}
          {filteredMetrics.length === 0 && (
            <Typography
              className={css.noResults}
              variant="caption"
              color="textSecondary"
              component="div"
            >
              No metrics to show.{` `}
              {searchText && (
                <span className={css.reset} onClick={() => setSearchText('')}>
                  Clear filter
                </span>
              )}
            </Typography>
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
};
