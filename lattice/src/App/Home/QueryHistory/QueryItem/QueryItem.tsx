import React, { FC, Fragment, useState } from 'react';
import Accordion from '@material-ui/core/Accordion';
import AccordionSummary from '@material-ui/core/AccordionSummary';
import AccordionDetails from '@material-ui/core/AccordionDetails';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import Moment from 'react-moment';
import { formatDuration } from 'shared/utils/formatDuration';
import { NodeIndicator } from '../NodeIndicator';
import css from './QueryItem.module.scss';

type QueryItemProps = {
  item: any;
  nodeIdx: number;
};

export const QueryItem: FC<QueryItemProps> = ({ item, nodeIdx }) => {
  const [expanded, setExpanded] = useState<boolean>(false);
  const [showFullQuery, setShowFullQuery] = useState<boolean>(false);
  const query = item.SQL ? item.SQL : `[${item.index}]${item.PQL}`;

  return (
    <Accordion
      expanded={expanded}
      onChange={() => setExpanded(!expanded)}
      className={css.item}
    >
      <AccordionSummary
        expandIcon={<ExpandMoreIcon />}
        classes={{ content: css.summary }}
      >
        <code className={css.queryHeader}>
          <span className={css.durationWrapper}>
            <span className={css.duration}>
              {formatDuration(item.runtimeNanoseconds, true)}
            </span>
          </span>
          <NodeIndicator node={item.nodeID} nodeIdx={nodeIdx} />
          <span className={css.queryString}>{query}</span>
        </code>
      </AccordionSummary>
      <AccordionDetails className={css.details}>
        <div className={css.metadata}>
          <div className={css.cell}>
            <label>Start Time</label>
            <div>
              <Moment date={item.start} format="MMM D, YYYY hh:mm:ss A" />
            </div>
          </div>
          <div className={css.cell}>
            <label>Index</label>
            <div>{item.index}</div>
          </div>
          <div className={css.cell}>
            <label>Node ID</label>
            {item.nodeID}
          </div>
        </div>
        <div>
          {showFullQuery ? (
            <Fragment>
              <code className={css.fullQuery}>{query}</code>
              {item.SQL ? (
                <div className={css.pqlTranslation}>
                  <label>PQL translation</label>
                  <code className={css.fullQuery}>{item.PQL}</code>
                </div>
              ) : null}
              <span
                className={css.showHideLink}
                onClick={() => setShowFullQuery(false)}
              >
                Hide Query
              </span>
            </Fragment>
          ) : (
            <span
              className={css.showHideLink}
              onClick={() => setShowFullQuery(true)}
            >
              Show Full Query
            </span>
          )}
        </div>
      </AccordionDetails>
    </Accordion>
  );
};
