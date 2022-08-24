import React, {
  createRef,
  FC,
  Fragment,
  useCallback,
  useEffect,
  useState
} from 'react';
import CircularProgress from '@material-ui/core/CircularProgress';
import Dialog from '@material-ui/core/Dialog';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import Paper from '@material-ui/core/Paper';
import Split from 'react-split';
import Typography from '@material-ui/core/Typography';
import { Block } from 'shared/Block';
import { Console } from './Console';
import { motion } from 'framer-motion';
import { QueryResults } from './QueryResults';
import { RecentQueries } from './RecentQueries';
import { ResultType } from './QueryContainer';
import './splitjs.scss';
import css from './Query.module.scss';

type QueryProps = {
  indexList: any[];
  results: ResultType[];
  error?: ResultType;
  loading: boolean;
  onRemoveResult: (resultIdx: number) => void;
  onClear: () => void;
  onQuery: (query: string, type: 'PQL' | 'SQL', index?: string) => void;
};

export const Query: FC<QueryProps> = ({
  indexList = [],
  results,
  error,
  loading,
  onRemoveResult,
  onClear,
  onQuery
}) => {
  const [index, setIndex] = useState<any>();
  const [helperText, setHelperText] = useState<string[]>([]);
  const [showRecent, setShowRecent] = useState<boolean>(false);
  const colSizes = JSON.parse(localStorage.getItem('colSizes') || '[80, 20]');
  const inputRef = createRef<HTMLInputElement>();
  const queries = JSON.parse(localStorage.getItem('recent-queries') || '[]');
  const hasRecentQueries = queries.length > 0;

  useEffect(() => {
    if (!error) {
      setIndex(undefined);
      setHelperText([]);
    }
  }, [results, error]);

  const onSetIndex = useCallback(
    (queryIndex: string | undefined) => {
      const matchIndex = indexList.find((i) => i.name === queryIndex);
      if (matchIndex) {
        setIndex(matchIndex);
      } else {
        setIndex(undefined);
      }
    },
    [setIndex, indexList]
  );

  const onReRunQuery = (query: string) => {
    const queryPattern = query.match(/\[([\w-]*?)\]/);
    if (query[0] === '[' || (queryPattern && queryPattern.index === 0)) {
      const indexStart = query.indexOf('[') + 1;
      const indexEnd = query.indexOf(']');
      const index = query.substring(indexStart, indexEnd);
      const pqlQuery = query.substring(indexEnd + 1);
      onQuery(pqlQuery, 'PQL', index);
    } else {
      onQuery(query, 'SQL');
    }
    setIndex(undefined);
  };

  return (
    <Fragment>
      <Block>
        <Typography variant="h5" color="textSecondary">
          Query
        </Typography>

        <Split
          sizes={colSizes}
          cursor="col-resize"
          minSize={215}
          onDragEnd={(sizes) =>
            localStorage.setItem('colSizes', JSON.stringify(sizes))
          }
          gutter={(_index, direction) => {
            const gutter = document.createElement('div');
            gutter.className = `gutter gutter-margin-top gutter-${direction}`;
            const dragbars = document.createElement('div');
            dragbars.className = 'dragBars';
            gutter.appendChild(dragbars);
            return gutter;
          }}
          style={{ display: 'flex' }}
        >
          <div className={css.consoleCol}>
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ duration: 0.7, delay: 0.3 }}
            >
              <div className={css.consoleLayout}>
                <div className={css.consoleContent}>
                  <Console
                    ref={inputRef}
                    onQuery={(
                      query: string,
                      type: 'PQL' | 'SQL',
                      index?: string
                    ) => onQuery(query, type, index)}
                    error={error}
                    onHelperTextChange={setHelperText}
                    onSetIndex={onSetIndex}
                    loading={loading}
                    indexList={indexList}
                    previousQueries={queries}
                  />

                  <Typography
                    className={css.syntaxHelper}
                    color="textSecondary"
                    variant="caption"
                    component="div"
                  >
                    {helperText
                      ? helperText.map((txt) => <div key={txt}>{txt}</div>)
                      : null}
                  </Typography>
                </div>
              </div>
            </motion.div>
            {results.length > 0 ? (
              <div className={css.clearResults}>
                <span
                  className={css.recentLink}
                  onClick={() => setShowRecent(true)}
                >
                  Show Recent Queries
                </span>
                <span className={css.divider}>|</span>
                <span className={css.clearLink} onClick={onClear}>
                  Clear Results
                </span>
              </div>
            ) : null}
            {!results.length && hasRecentQueries ? (
              <Fragment>
                <Typography color="textPrimary" variant="overline">
                  Recent Queries
                </Typography>
                <RecentQueries onQueryClick={(query) => onReRunQuery(query)} />
              </Fragment>
            ) : null}
            {loading && (
              <Paper className={css.loadingContainer}>
                <CircularProgress size={20} />
              </Paper>
            )}
            {results.map((result, idx) => (
              <Paper key={`query-result-${idx}`} className={css.results}>
                <QueryResults
                  collapsibleQuery={false}
                  results={result}
                  onRemoveResult={() => onRemoveResult(idx)}
                />
              </Paper>
            ))}
          </div>
          {index ? (
            <div className={css.fieldsCol}>
              <Typography color="textPrimary" variant="overline">
                Available Fields
              </Typography>
              <Typography
                className={css.fieldList}
                color="textSecondary"
                variant="caption"
                component="div"
              >
                {index.fields.map((field) => (
                  <div
                    key={field.name}
                    title={`${field.name} (${field.options.type})`}
                  >
                    {field.name} ({field.options.type})
                  </div>
                ))}
              </Typography>
            </div>
          ) : (
            <div className={css.fieldsCol}>
              <Typography color="textPrimary" variant="overline">
                Available Indexes
              </Typography>
              {indexList ? (
                <Typography
                  className={css.fieldList}
                  color="textSecondary"
                  variant="caption"
                  component="div"
                >
                  {indexList.map((i) => (
                    <div key={i.name} title={i.name}>
                      {i.name}
                    </div>
                  ))}
                </Typography>
              ) : (
                <Typography
                  color="textSecondary"
                  variant="caption"
                  component="div"
                >
                  No indexes available
                </Typography>
              )}
            </div>
          )}
        </Split>
      </Block>
      <Dialog
        open={showRecent}
        onClose={() => setShowRecent(false)}
        maxWidth="md"
      >
        <DialogTitle>Recent Queries</DialogTitle>
        <DialogContent>
          <div className={css.list}>
            <RecentQueries
              onQueryClick={(query) => {
                setShowRecent(false);
                onReRunQuery(query);
              }}
            />
          </div>
        </DialogContent>
      </Dialog>
    </Fragment>
  );
};
