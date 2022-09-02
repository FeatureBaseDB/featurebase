import React, { FC } from 'react';
import DeleteIcon from '@material-ui/icons/Delete';
import IconButton from '@material-ui/core/IconButton';
import List from '@material-ui/core/List';
import ListItem from '@material-ui/core/ListItem';
import ListItemSecondaryAction from '@material-ui/core/ListItemSecondaryAction';
import ListItemText from '@material-ui/core/ListItemText';
import Paper from '@material-ui/core/Paper';
import Tooltip from '@material-ui/core/Tooltip';
import WarningIcon from '@material-ui/icons/Warning';
import { MotionGroup, MotionSlideItem } from 'shared/Animations';
import { Operator, RowGrouping } from '../rowTypes';

type SavedQueriesProps = {
  queries: {
    name: string;
    description?: string;
    table: string;
    operation: string;
    operator?: Operator;
    rowCalls: RowGrouping[];
    isInvalid?: boolean;
  }[];
  tables: any[];
  onClickSaved: (queryIdx: number) => void;
  onRemoveQuery?: (queryIdx: number) => void;
};

export const SavedQueries: FC<SavedQueriesProps> = ({
  queries,
  tables,
  onClickSaved,
  onRemoveQuery
}) => {
  const tablesList = tables.map((table) => table.name);

  return (
    <Paper>
      <List>
        {queries.length > 0 ? (
          <MotionGroup>
            {queries.map((query, idx) => {
              const { table, name, description } = query;
              const tableExists = tablesList.includes(query.table);

              return (
                <MotionSlideItem key={name}>
                  <ListItem
                    role={undefined}
                    onClick={() => onClickSaved(idx)}
                    disabled={!tableExists}
                    dense
                    button
                  >
                    <ListItemText
                      primary={name}
                      secondary={`[${table}] ${description}`}
                    />
                    <ListItemSecondaryAction>
                      {!tableExists ? (
                        <Tooltip
                          title="Table does not exist on this pilosa cluster."
                          placement="top"
                          arrow
                        >
                          <span>
                            <IconButton edge="end" disabled>
                              <WarningIcon color="error" />
                            </IconButton>
                          </span>
                        </Tooltip>
                      ) : null}
                      {onRemoveQuery ? (
                        <IconButton
                          edge="end"
                          aria-label="Delete saved query"
                          onClick={() => onRemoveQuery(idx)}
                        >
                          <DeleteIcon />
                        </IconButton>
                      ) : null}
                    </ListItemSecondaryAction>
                  </ListItem>
                </MotionSlideItem>
              );
            })}
          </MotionGroup>
        ) : null}
      </List>
    </Paper>
  );
};
