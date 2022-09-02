import React, { FC } from 'react';
import groupBy from 'lodash/groupBy';
import { ResponsiveBar } from '@nivo/bar';
import { ResultType } from '../../Query';
import { schemeTableau10 } from 'd3-scale-chromatic';
import { useTheme } from '@material-ui/core/styles';

type GroupByChartType = {
  results: ResultType;
};

export const GroupByChart: FC<GroupByChartType> = ({ results }) => {
  const theme = useTheme();
  const isDark = theme.palette.type === 'dark';
  const { headers, rows } = results;
  let uniqueKeys: string[] = [];
  const grouped = groupBy(rows, (row) => row[0].stringval);
  const data = Object.keys(grouped).map((key) => {
    let groupData = {};
    grouped[key].forEach((row) => {
      const secondaryValue = row[1][`${headers[1].datatype}val`];
      if (headers.length > 2) {
        groupData = {
          ...groupData,
          [headers[0].name]: row[0][`${headers[0].datatype}val`],
          [secondaryValue]: row[2][`${headers[2].datatype}val`],
        };

        if (!uniqueKeys.includes(secondaryValue.toString())) {
          uniqueKeys.push(secondaryValue.toString());
        }
      } else {
        groupData = {
          ...groupData,
          [headers[0].name]: row[0][`${headers[0].datatype}val`],
          value: secondaryValue,
        };
      }
    });

    return groupData;
  });

  return (
    <div style={{ height: '80%' }}>
      <ResponsiveBar
        data={data}
        keys={uniqueKeys.length > 0 ? uniqueKeys : undefined}
        indexBy={headers[0].name}
        margin={{ top: 50, right: 130, bottom: 100, left: 60 }}
        padding={0.3}
        valueScale={{ type: 'linear' }}
        indexScale={{ type: 'band', round: true }}
        colors={schemeTableau10}
        enableLabel={false}
        theme={
          isDark
            ? {
                textColor: 'var(--text-secondary)',
                axis: {
                  domain: { line: { stroke: 'rgba(255, 255, 255, 0.1)' } },
                },
                grid: { line: { stroke: 'rgba(255, 255, 255, 0.1)' } },
                tooltip: { container: { background: '#1c2022' } },
              }
            : {
                axis: {
                  domain: { line: { stroke: '#dddddd' } },
                },
              }
        }
        groupMode="grouped"
        axisBottom={{
          tickSize: 5,
          tickPadding: 5,
          tickRotation: -40,
          legend: headers[0].name,
          legendPosition: 'middle',
          legendOffset: 80,
        }}
        axisLeft={{
          tickSize: 5,
          tickPadding: 5,
          tickRotation: 0,
          legend: 'count',
          legendPosition: 'middle',
          legendOffset: -40,
        }}
        tooltip={({ id, value, color }) => (
          <strong style={{ color }}>
            {id}: {value}
          </strong>
        )}
        labelSkipWidth={12}
        labelSkipHeight={12}
        labelTextColor={{ from: 'color', modifiers: [['darker', 1.6]] }}
        legends={[
          {
            dataFrom: 'keys',
            anchor: 'bottom-right',
            direction: 'column',
            justify: false,
            translateX: 120,
            translateY: 0,
            itemsSpacing: 2,
            itemWidth: 120,
            itemHeight: 20,
            itemDirection: 'left-to-right',
            itemOpacity: 0.85,
            symbolShape: 'circle',
            symbolSize: 20,
            effects: [
              {
                on: 'hover',
                style: {
                  itemOpacity: 1,
                },
              },
            ],
          },
        ]}
        animate={true}
        motionStiffness={90}
        motionDamping={15}
      />
    </div>
  );
};
