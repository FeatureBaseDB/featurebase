import React, { FC, Fragment, useEffect, useState } from 'react';
import sub from 'date-fns/sub';
import format from 'date-fns/format';
import isUndefined from 'lodash/isUndefined';
import Typography from '@material-ui/core/Typography';
import { ResponsiveLine as Line } from '@nivo/line';
import { metricsList } from './helpers';
import { useTheme } from '@material-ui/core/styles';
import css from './ImportStatus.module.scss';
import add from 'date-fns/add';

type ImportStatusType = {
  metrics: any;
};

export const ImportStatus: FC<ImportStatusType> = ({ metrics }) => {
  const theme = useTheme();
  const isDark = theme.palette.type === 'dark';
  const [time, setTime] = useState<Date>(new Date());
  const [data, setData] = useState<any>();

  useEffect(() => {
    const nodes = Object.keys(metrics);
    let newData = data ? { ...data } : {};
    let importMetrics = {};

    nodes.forEach((node) => {
      metrics[node].forEach((metric) => {
        if (metricsList.includes(metric.name)) {
          let aggregateValue = importMetrics[metric.name]
            ? importMetrics[metric.name].value
            : 0;
          metric.metrics.forEach((m) => {
            aggregateValue = aggregateValue + Number(m.value);
          });

          importMetrics = {
            ...importMetrics,
            [metric.name]: {
              x: time,
              value: aggregateValue
            }
          };
        }
      });
    });

    metricsList.forEach((metric) => {
      if (!newData[metric]) {
        newData[metric] = [];
      }

      if (importMetrics[metric]) {
        newData[metric] = [...newData[metric], importMetrics[metric]];
      } else {
        newData[metric] = [
          ...newData[metric],
          {
            x: time,
            y: 0,
            value: 0
          }
        ];
      }

      newData[metric].forEach((node, i) => {
        const prevValue = i > 0 ? newData[metric][i - 1].value : undefined;

        if (isUndefined(prevValue)) {
          newData[metric][i] = { ...newData[metric][i], y: 0 };
        } else {
          newData[metric][i] = {
            ...newData[metric][i],
            y: ((node.value - prevValue) / 15).toFixed(1)
          };
        }
      });

      if (newData[metric].length > 60) {
        newData[metric].splice(0, 1);
      }
    });

    setTime((t) => add(t, { seconds: 15 }));
    setData(newData);
  }, [metrics]);

  return (
    <Fragment>
      <Typography variant="h5" color="textSecondary">
        Activity
      </Typography>
      {data ? (
        <div className={css.activityLayout}>
          <div>
            <Line
              colors={{ scheme: 'dark2' }}
              theme={
                isDark
                  ? {
                      textColor: 'var(--text-secondary)',
                      axis: {
                        domain: { line: { stroke: 'rgba(255, 255, 255, 0.1)' } }
                      },
                      grid: { line: { stroke: 'rgba(255, 255, 255, 0.1)' } },
                      tooltip: { container: { background: '#1c2022' } }
                    }
                  : {
                      axis: {
                        domain: { line: { stroke: '#dddddd' } }
                      }
                    }
              }
              margin={{ top: 20, right: 20, bottom: 45, left: 80 }}
              data={[
                {
                  id: 'pilosa_set_bit_total (set via PQL)',
                  data: data['pilosa_set_bit_total']
                },
                {
                  id: 'pilosa_importing_total (set requests via import API)',
                  data: data['pilosa_importing_total']
                },
                {
                  id:
                    'pilosa_imported_total (actual changed bits via import API)',
                  data: data['pilosa_imported_total']
                }
              ]}
              axisLeft={{
                // TODO: potentially format ticks values differently
                // format: (value) => formatBytes(Number(value), 0)
                legend: 'bits per sec',
                legendPosition: 'middle',
                legendOffset: -55,
                format: (value) => value.toLocaleString()
              }}
              yFormat={(value) => `${value.toLocaleString()} bits/sec`}
              enableGridX={false}
              xScale={{ type: 'time', min: sub(time, { minutes: 15 }) }}
              axisBottom={{
                legend: 'Import (set) stats',
                legendPosition: 'middle',
                legendOffset: 35,
                tickValues: 4,
                format: (value) => format(value as Date, 'h:mm aaaa')
              }}
              enableSlices="x"
              sliceTooltip={({ slice }) => (
                <div className={css.tooltip}>
                  <Typography variant="body2" color="textSecondary" paragraph>
                    <strong>
                      {format(
                        slice.points[0].data.x as Date,
                        'M/d/yyyy h:mm:ss aaaa'
                      )}
                    </strong>
                  </Typography>
                  <div>
                    {slice.points.map((p) => {
                      const label = p.serieId.toString();
                      const splitIdx = label.indexOf(' ');

                      return (
                        <div key={p.serieId} className={css.metric}>
                          <div
                            className={css.metricName}
                            style={{ color: p.serieColor }}
                          >
                            <strong>{label.substring(0, splitIdx)}</strong>
                            <Typography variant="caption" component="div">
                              {label.substring(splitIdx + 1)}
                            </Typography>
                          </div>
                          <span style={{ color: p.serieColor }}>
                            {p.data.yFormatted}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}
              curve="monotoneX"
              animate={false}
            />
          </div>
          <div>
            <Line
              colors={{ scheme: 'dark2' }}
              theme={
                isDark
                  ? {
                      textColor: 'var(--text-secondary)',
                      axis: {
                        domain: { line: { stroke: 'rgba(255, 255, 255, 0.1)' } }
                      },
                      grid: { line: { stroke: 'rgba(255, 255, 255, 0.1)' } },
                      tooltip: { container: { background: '#1c2022' } }
                    }
                  : {
                      axis: {
                        domain: { line: { stroke: '#dddddd' } }
                      }
                    }
              }
              margin={{ top: 20, right: 20, bottom: 45, left: 80 }}
              data={[
                {
                  id: 'pilosa_clear_bit_total ',
                  data: data['pilosa_clear_bit_total']
                },
                {
                  id: 'pilosa_clearing_total ',
                  data: data['pilosa_clearing_total']
                },
                {
                  id: 'pilosa_cleared_total ',
                  data: data['pilosa_cleared_total']
                }
              ]}
              axisLeft={{
                // TODO: potentially format ticks values differently
                // format: (value) => formatBytes(Number(value), 0)
                legend: 'bits per sec',
                legendPosition: 'middle',
                legendOffset: -55,
                format: (value) => value.toLocaleString()
              }}
              yFormat={(value) => `${value.toLocaleString()} bits/sec`}
              enableGridX={false}
              xScale={{ type: 'time', min: sub(time, { minutes: 15 }) }}
              axisBottom={{
                legend: 'Import (clear) stats',
                legendPosition: 'middle',
                legendOffset: 35,
                tickValues: 4,
                format: (value) => format(value as Date, 'h:mm aaaa')
              }}
              enableSlices="x"
              sliceTooltip={({ slice }) => (
                <div className={css.tooltip}>
                  <Typography variant="body2" color="textSecondary" paragraph>
                    <strong>
                      {format(
                        slice.points[0].data.x as Date,
                        'M/d/yyyy h:mm:ss aaaa'
                      )}
                    </strong>
                  </Typography>
                  <div>
                    {slice.points.map((p) => {
                      const label = p.serieId.toString();
                      const splitIdx = label.indexOf(' ');

                      return (
                        <div key={p.serieId} className={css.metric}>
                          <div
                            className={css.metricName}
                            style={{ color: p.serieColor }}
                          >
                            <strong>{label.substring(0, splitIdx)}</strong>
                            <Typography variant="caption" component="div">
                              {label.substring(splitIdx + 1)}
                            </Typography>
                          </div>
                          <span style={{ color: p.serieColor }}>
                            {p.data.yFormatted}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}
              curve="monotoneX"
              animate={false}
            />
          </div>
        </div>
      ) : null}
    </Fragment>
  );
};
