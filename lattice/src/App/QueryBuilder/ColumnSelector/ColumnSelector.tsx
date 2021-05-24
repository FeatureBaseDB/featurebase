import React, { FC, useEffect, useState } from 'react';
import Button from '@material-ui/core/Button';
import classNames from 'classnames';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import DragIndicatorIcon from '@material-ui/icons/DragIndicator';
import isEqual from 'lodash/isEqual';
import List from '@material-ui/core/List';
import ListItem from '@material-ui/core/ListItem';
import Paper from '@material-ui/core/Paper';
import Typography from '@material-ui/core/Typography';
import {
  DragDropContext,
  Droppable,
  Draggable,
  DroppableProvided,
  DraggableProvided,
  DropResult,
  DraggableLocation,
  DroppableStateSnapshot
} from 'react-beautiful-dnd';
import css from './ColumnSelector.module.scss';

type ColumnSelectorProps = {
  open: boolean;
  fieldsList: { name: string; show: boolean }[];
  onChange: (updatedList: { name: string; show: boolean }[]) => void;
  onClose: () => void;
};

export const ColumnSelector: FC<ColumnSelectorProps> = ({
  open,
  fieldsList,
  onChange,
  onClose
}) => {
  const [updatedList, setUpdatedList] = useState<{
    show: string[];
    hide: string[];
  }>({ show: [], hide: [] });

  useEffect(() => {
    setUpdatedList({
      show: fieldsList.filter((f) => f.show).map((f) => f.name),
      hide: fieldsList.filter((f) => !f.show).map((f) => f.name)
    });
  }, [fieldsList]);

  const onUpdateColumns = () => {
    let list: { name: string; show: boolean }[] = [];
    updatedList.show.forEach((item) => list.push({ name: item, show: true }));
    updatedList.hide.forEach((item) => list.push({ name: item, show: false }));

    if (isEqual(fieldsList, list)) {
      onClose();
    } else {
      onChange(list);
      onClose();
    }
  };

  const onCloseSelector = () => {
    setUpdatedList({
      show: fieldsList.filter((f) => f.show).map((f) => f.name),
      hide: fieldsList.filter((f) => !f.show).map((f) => f.name)
    });
    onClose();
  }

  const reorder = (list: string[], startIdx: number, endIdx: number) => {
    const result = Array.from(list);
    const [removed] = result.splice(startIdx, 1);
    result.splice(endIdx, 0, removed);

    return result;
  };

  const move = (source: DraggableLocation, dest: DraggableLocation) => {
    const sourceType = source.droppableId === 'show' ? 'show' : 'hide';
    const destType = dest.droppableId === 'show' ? 'show' : 'hide';
    const sourceClone = [...updatedList[sourceType]];
    const destClone = [...updatedList[destType]];
    const [removed] = sourceClone.splice(source.index, 1);

    destClone.splice(dest.index, 0, removed);

    const result = { ...updatedList };
    result[sourceType] = sourceClone;
    result[destType] = destClone;

    return result;
  };

  const onDragEnd = (result: DropResult) => {
    const { source, destination } = result;

    if (!destination) {
      return;
    }

    if (source.droppableId === destination.droppableId) {
      const listType = source.droppableId === 'show' ? 'show' : 'hide';
      const newList = reorder(
        [...updatedList[listType]],
        source.index,
        destination.index
      );

      setUpdatedList({ ...updatedList, [listType]: newList });
    } else {
      const newList = move(source, destination);
      setUpdatedList(newList);
    }
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="lg" disableBackdropClick>
      <DialogTitle>Configure Result Fields</DialogTitle>
      <DialogContent>
        <Typography variant="caption" color="textSecondary" paragraph>
          Drag and drop fields to reorder, show or hide fields.
        </Typography>
        <div className={css.layout}>
          <Typography variant="overline">Show Fields</Typography>
          <Typography variant="overline">Hide Fields</Typography>
          <DragDropContext onDragEnd={onDragEnd}>
            <Droppable droppableId="show">
              {(
                provided: DroppableProvided,
                snapshot: DroppableStateSnapshot
              ) => (
                <Paper
                  ref={provided.innerRef}
                  {...provided.droppableProps}
                  className={classNames(css.droppable, {
                    [css.isDraggingOver]: snapshot.isDraggingOver
                  })}
                >
                  <List dense>
                    {updatedList.show.map((field, idx) => (
                      <Draggable key={field} draggableId={field} index={idx}>
                        {(provided: DraggableProvided) => (
                          <ListItem
                            ref={provided.innerRef}
                            {...provided.draggableProps}
                            {...provided.dragHandleProps}
                          >
                            <DragIndicatorIcon
                              className={css.dragHandle}
                              fontSize="small"
                              color="disabled"
                            />
                            {field}
                          </ListItem>
                        )}
                      </Draggable>
                    ))}
                    {provided.placeholder}
                  </List>
                </Paper>
              )}
            </Droppable>
            <Droppable droppableId="hide">
              {(
                provided: DroppableProvided,
                snapshot: DroppableStateSnapshot
              ) => (
                <Paper
                  ref={provided.innerRef}
                  {...provided.droppableProps}
                  className={classNames(css.droppable, {
                    [css.isDraggingOver]: snapshot.isDraggingOver
                  })}
                >
                  <List dense>
                    {updatedList.hide.map((field, idx) => (
                      <Draggable key={field} draggableId={field} index={idx}>
                        {(provided: DraggableProvided) => (
                          <ListItem
                            ref={provided.innerRef}
                            {...provided.draggableProps}
                            {...provided.dragHandleProps}
                          >
                            <DragIndicatorIcon
                              className={css.dragHandle}
                              fontSize="inherit"
                              color="disabled"
                            />
                            {field}
                          </ListItem>
                        )}
                      </Draggable>
                    ))}
                    {provided.placeholder}
                  </List>
                </Paper>
              )}
            </Droppable>
          </DragDropContext>
        </div>
      </DialogContent>
      <DialogActions>
        <Button onClick={onCloseSelector}>Cancel</Button>
        <Button variant="contained" color="primary" onClick={onUpdateColumns}>
          Apply
        </Button>
      </DialogActions>
    </Dialog>
  );
};
