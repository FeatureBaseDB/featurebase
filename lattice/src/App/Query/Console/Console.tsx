import React, {
  ChangeEvent,
  forwardRef,
  Fragment,
  KeyboardEvent,
  SyntheticEvent,
  useCallback,
  useEffect,
  useRef,
  useState
} from 'react';
import ChevronRightIcon from '@material-ui/icons/ChevronRight';
import classNames from 'classnames';
import ClearIcon from '@material-ui/icons/Clear';
import Fuse from 'fuse.js';
import IconButton from '@material-ui/core/IconButton';
import InputBase from '@material-ui/core/InputBase';
import SubdirectoryIcon from '@material-ui/icons/SubdirectoryArrowLeft';
import Tooltip from '@material-ui/core/Tooltip';
import Typography from '@material-ui/core/Typography';
import { Block } from 'shared/Block';
import {
  keywordHelpers,
  pqlKeywords,
  rowCallOptions,
  sqlKeywords
} from './helpers';
import { ResultType } from 'App/Query/QueryContainer';
import css from './Console.module.scss';

type ConsoleProps = {
  loading: boolean;
  indexList: any[];
  previousQueries: string[];
  error?: ResultType;
  onQuery: (query: string, type: 'PQL' | 'SQL', index?: string) => void;
  onHelperTextChange: (helperText: string[]) => void;
  onSetIndex: (index: string | undefined) => void;
};

export const Console = forwardRef((props: ConsoleProps, ref: any) => {
  const {
    loading,
    indexList,
    previousQueries,
    error,
    onQuery,
    onHelperTextChange,
    onSetIndex
  } = props;
  const [inputVal, setInputVal] = useState<string>('');
  const [showError, setShowError] = useState<boolean>(false);
  const [autocomplete, setAutocomplete] = useState<string>('');
  const [showAutocomplete, setShowAutocomplete] = useState<boolean>(false);
  const [inputIsDirty, setInputIsDirty] = useState<boolean>(false);
  const [clearedAfterQuery, setClearedAfterQuery] = useState<boolean>(true);
  const [queryType, setQueryType] = useState<'PQL' | 'SQL'>('SQL');
  const [queryHistory, setQueryHistory] = useState<number>(-1);
  const [focused, setFocused] = useState<boolean>(false);
  const keys = Object.keys(keywordHelpers);
  const inputEl = useRef<HTMLInputElement>(null);

  const clearInput = useCallback(() => {
    if (ref.current) {
      ref.current.value = '';
      setInputIsDirty(false);
      setQueryHistory(-1);
      setShowError(false);
      setAutocomplete('');
      ref.current.focus();
    }
  }, [ref]);

  useEffect(() => {
    if (!loading && !error && !clearedAfterQuery) {
      clearInput();
      setClearedAfterQuery(true);
      setShowError(false);
    } else if (error) {
      ref.current.focus();
    }
  }, [ref, loading, clearInput, clearedAfterQuery, error, setShowError]);

  useEffect(() => {
    if (error) {
      setShowError(true);
    }
  }, [error]);

  const onQueryEnter = (event: SyntheticEvent) => {
    event.preventDefault();
    if (ref && ref.current && inputIsDirty) {
      if (queryType === 'PQL') {
        const query = ref.current.value.trim();
        const indexStart = query.indexOf('[') + 1;
        const indexEnd = query.indexOf(']');
        const index = query.substring(indexStart, indexEnd);
        const pqlQuery = query.substring(indexEnd + 1);
        onQuery(pqlQuery.replace(/(?<!;)(?=(;+))\1+$/, ''), queryType, index);
      } else {
        onQuery(ref.current.value, queryType);
      }
    }
  };

  // [syang] tbh, this function does too much at the moment since it
  // handles autocomplete for both PQL and SQL as well as showing the
  // helper text and detecting when a table has been selected.

  // TODO: if we're keeping autocomplete functionality a front end
  // thing, we should consider using a parser strategy since it will
  // be easier to manage with addition of future functionality
  // ie: https://pegjs.org/
  const onInputChange = (event: ChangeEvent<HTMLInputElement>) => {
    const value = event.target.value;
    const trimmedValue = event.target.value.trim();
    setShowError(false);
    setInputVal(value);
    setQueryHistory(-1);
    let pqlPattern;

    if (trimmedValue !== '') {
      pqlPattern = trimmedValue.match(/\[([\w-]*?)\]/);
      setInputIsDirty(true);
    } else {
      setInputIsDirty(false);
    }

    if (trimmedValue[0] === '[' || (pqlPattern && pqlPattern.index === 0)) {
      setQueryType('PQL');
    } else {
      setQueryType('SQL');
    }

    updateHelper(trimmedValue);

    if (pqlPattern && pqlPattern.index === 0) {
      onSetIndex(pqlPattern[1]);

      const queryString = value.substring(value.indexOf(']') + 1).trim();
      const operationIndex = queryString.indexOf('(');
      if (operationIndex < 0) {
        setAutocompleteText(pqlKeywords, false, queryString);
      } else {
        const index = indexList.find((i) => i.name === pqlPattern[1]);
        const { op, postOpString } = getOperation(queryString);

        switch (op) {
          case 'Row':
            if (postOpString.includes(',')) {
              const indexOfComma = postOpString.indexOf(',');
              const postCommaString = postOpString
                .substring(indexOfComma + 1)
                .trimStart();
              const indexOfFrom = postCommaString.indexOf('from=');
              const indexOfTo = postCommaString.indexOf('to=');
              if (indexOfFrom >= 0 && indexOfTo >= 0) {
                setAutocomplete('');
              } else if (
                postCommaString.indexOf(',') >= 0 &&
                indexOfFrom >= 0
              ) {
                const argString = postCommaString
                  .substring(postCommaString.indexOf(',') + 1)
                  .trimStart();
                if (argString.length === 0) {
                  setAutocomplete('to=');
                } else {
                  setAutocompleteText(['to='], false, argString);
                }
              } else if (postCommaString.indexOf(',') >= 0 && indexOfTo >= 0) {
                const argString = postCommaString
                  .substring(postCommaString.indexOf(',') + 1)
                  .trimStart();
                if (argString.length === 0) {
                  setAutocomplete('from=');
                } else {
                  setAutocompleteText(['from='], false, argString);
                }
              } else {
                setAutocompleteText(['from=', 'to='], false, postCommaString);
              }
            } else {
              setAutocompleteText(index.fields, true, postOpString);
            }
            break;
          case 'Rows':
            if (postOpString.includes(',')) {
              const lastIndexOfComma = postOpString.lastIndexOf(',');
              const postCommaString = postOpString
                .substring(lastIndexOfComma + 1)
                .trimStart();
              setAutocompleteText(
                ['previous=', 'limit=', 'column=', 'from=', 'to='],
                false,
                postCommaString
              );
            } else {
              setAutocompleteText(index.fields, true, postOpString);
            }
            break;
          case 'Clear':
          case 'Set':
            if (postOpString.includes(',')) {
              const indexOfComma = postOpString.indexOf(',');
              const postCommaString = postOpString
                .substring(indexOfComma + 1)
                .trimStart();
              setAutocompleteText(index.fields, true, postCommaString);
            } else {
              setAutocomplete('');
            }
            break;
          case 'SetRowAttrs':
            if (postOpString.includes(',')) {
              const indexOfComma = postOpString.indexOf(',');
              const postCommaString = postOpString
                .substring(indexOfComma + 1)
                .trimStart();
              setAutocompleteText(rowCallOptions, false, postCommaString);
            } else {
              setAutocompleteText(index.fields, true, postOpString);
            }
            break;
          case 'Store':
            if (postOpString.includes(',')) {
              const indexOfComma = postOpString.indexOf(',');
              const postCommaString = postOpString
                .substring(indexOfComma + 1)
                .trimStart();
              setAutocompleteText(index.fields, true, postCommaString);
            } else {
              setAutocompleteText(rowCallOptions, false, postOpString);
            }
            break;
          case 'ClearRow':
            setAutocompleteText(index.fields, true, postOpString);
            break;
          case 'TopK':
            if (postOpString.includes(',')) {
              const indexOfComma = postOpString.lastIndexOf(',');
              const postCommaString = postOpString
                .substring(indexOfComma + 1)
                .trimStart();
              setAutocompleteText(
                [...rowCallOptions, 'k=', 'filter=', 'from=', 'to='],
                false,
                postCommaString
              );
            } else {
              setAutocompleteText(index.fields, true, postOpString);
            }
            break;
          case 'TopN':
            if (postOpString.includes(',')) {
              const indexOfComma = postOpString.lastIndexOf(',');
              const postCommaString = postOpString
                .substring(indexOfComma + 1)
                .trimStart();
              setAutocompleteText(
                [...rowCallOptions, 'n=', 'attrName=', 'attrValues='],
                false,
                postCommaString
              );
            } else {
              setAutocompleteText(index.fields, true, postOpString);
            }
            break;
          case 'Min':
          case 'Max':
          case 'Sum':
            if (postOpString.includes('field=')) {
              const lastIndexOfEquals = postOpString.lastIndexOf('=');
              const postEquals = postOpString
                .substring(lastIndexOfEquals + 1)
                .trimStart();
              setAutocompleteText(index.fields, true, postEquals, true);
            } else if (postOpString.includes(',')) {
              const openParen = postOpString.match(/\(/g) || [];
              const closeParen = postOpString.match(/\)/g) || [];
              const lastIndexOfComma = postOpString.lastIndexOf(',');
              const postCommaString = postOpString
                .substring(lastIndexOfComma + 1)
                .trimStart();

              if (closeParen.length - openParen.length > 0) {
                setAutocompleteText(rowCallOptions, false, postCommaString);
              } else if (closeParen.length - openParen.length === 0) {
                if (postCommaString.trim().length === 0) {
                  setAutocomplete('field=');
                } else {
                  setAutocompleteText(['field='], false, postCommaString);
                }
              } else {
                setAutocomplete('');
              }
            } else {
              setAutocompleteText(
                [...rowCallOptions, 'field='],
                false,
                postOpString
              );
            }
            break;
          case 'Difference':
          case 'Intersect':
          case 'Union':
          case 'Xor':
            if (postOpString.includes(',')) {
              const lastIndexOfComma = postOpString.lastIndexOf(',');
              const postCommaString = postOpString
                .substring(lastIndexOfComma + 1)
                .trimStart();
              setAutocompleteText(rowCallOptions, false, postCommaString);
            } else {
              setAutocompleteText(rowCallOptions, false, postOpString);
            }
            break;
          case 'Limit':
          case 'Not':
          case 'Count':
          case 'Extract':
          case 'IncludesColumn':
            if (postOpString.match(/(.*?)/)) {
              const indexOfClose = postOpString.indexOf(')');
              const postCloseString = postOpString
                .substring(indexOfClose + 1)
                .trimStart();
              setAutocompleteText([', column='], false, postCloseString);
            } else {
              setAutocompleteText(rowCallOptions, false, postOpString);
            }
            break;
          case 'UnionRows':
            if (postOpString.includes(',')) {
              const lastIndexOfComma = postOpString.lastIndexOf(',');
              const postCommaString = postOpString
                .substring(lastIndexOfComma + 1)
                .trimStart();
              setAutocompleteText(['Rows('], false, postCommaString);
            } else {
              setAutocompleteText(['Rows('], false, postOpString);
            }
            break;
          case 'Options':
            if (postOpString.includes(',')) {
              const lastIndexOfComma = postOpString.lastIndexOf(',');
              const postCommaString = postOpString
                .substring(lastIndexOfComma + 1)
                .trimStart();
              setAutocompleteText(
                [
                  'columnAttrs=',
                  'excludeColumns=',
                  'excludeRowAttrs=',
                  'shards='
                ],
                false,
                postCommaString
              );
            } else {
              setAutocompleteText(pqlKeywords, false, postOpString);
            }
            break;
          default:
            setAutocomplete('');
            break;
        }
      }
    } else {
      if (queryType === 'PQL') {
        onSetIndex(undefined);
        if (!pqlPattern) {
          setAutocompleteText(
            indexList,
            true,
            trimmedValue.substring(1),
            false,
            true
          );
        } else {
          setAutocomplete('');
        }
      } else {
        const split = value.split(' ');
        switch (split[0]) {
          case 'drop':
            if (value.match(/drop(\s+)table(\s+)/)) {
              const postOpString = value.substring(11);
              setAutocompleteText(
                indexList,
                true,
                postOpString.trim().replace('`', '')
              );
            } else {
              setAutocompleteText(sqlKeywords, false, value);
            }
            break;
          case 'show':
            if (value.match(/show(\s+)fields(\s+)from(\s+)/)) {
              const postOpString = value.substring(17);
              setAutocompleteText(
                indexList,
                true,
                postOpString.trim().replace('`', '')
              );
            } else {
              setAutocompleteText(sqlKeywords, false, value);
            }
            break;
          case 'select':
            if (value.includes('distinct ') && value.includes('from ')) {
              const fromIdx = value.indexOf('from ');
              const postFromString = value.substring(fromIdx + 5);
              setAutocompleteText(indexList, true, postFromString.trim());
            } else if (!value.includes('distinct ') && value.includes('from ') && value.includes('where ')) {
              const whereIdx = value.indexOf('where ');
              const postWhereString = value.substring(whereIdx + 6);
              const indexMatch = value.match(/from (.*) where(\s+)/);
              if (indexMatch) {
                const index = indexList.find(
                  (i) => i.name === indexMatch[1].trim().replaceAll('`', '')
                );
                if (index) {
                  onSetIndex(index.name);
                  if (postWhereString.includes('=') && postWhereString.includes('and')) {
                    const lastSpaceIdx = postWhereString.lastIndexOf(' ');
                    const postAndString = postWhereString
                      .substring(lastSpaceIdx + 1)
                      .trim();
                    setAutocompleteText(
                      [...index.fields, { name: 'and ' }],
                      true,
                      postAndString.replaceAll('`', '')
                    );
                  } else if (postWhereString.includes('=') ) {
                    const lastSpaceIdx = postWhereString.lastIndexOf(' ');
                    const postFieldString = postWhereString
                      .substring(lastSpaceIdx + 1)
                      .trim();
                    setAutocompleteText(['and '], false, postFieldString);
                  } else {
                    setAutocompleteText(
                      index.fields,
                      true,
                      postWhereString.trim().replaceAll('`', '')
                    );
                  }
                }
              } else {
                setAutocomplete('');
              }
            } else if (!value.includes('distinct ') && value.includes('from ')) {
              const fromIdx = value.indexOf('from ');
              const postFromString = value.substring(fromIdx + 5).trimStart();
              const spaceIdx = postFromString.trimStart().indexOf(' ');
              if (spaceIdx > 0) {
                const indexString = postFromString
                  .substring(0, spaceIdx)
                  .replaceAll('`', '');
                const index = indexList.find((i) => i.name === indexString);
                if (index) {
                  onSetIndex(indexString);
                }
                const postSpaceString = postFromString.substring(spaceIdx);
                setAutocompleteText(
                  ['where ', 'limit '],
                  false,
                  postSpaceString.trim()
                );
              } else {
                setAutocompleteText(
                  indexList,
                  true,
                  postFromString.trim().replace('`', '')
                );
              }
            } else {
              onSetIndex(undefined);
              setAutocompleteText(sqlKeywords, false, value);
            }
            break;
          default:
            onSetIndex(undefined);
            setAutocompleteText(sqlKeywords, false, value);
            break;
        }
      }
    }
  };

  const getOperation = (str: string) => {
    let matchesOpen: any[] = [];
    const rx = /\(/g;
    let match;
    while ((match = rx.exec(str)) !== null) {
      matchesOpen.push(match.index);
    }

    let matchStack: any[] = [];
    let openCount = 0;
    const brackets = str.match(/[()]/g) || [];
    const opsList = str.split('(');
    for (let i = 0; i < brackets.length; i++) {
      if (brackets[i] === '(') {
        matchStack.push(openCount);
        openCount++;
      } else {
        matchStack.pop();
      }
    }
    const matchIndex = matchStack.length
      ? matchStack[matchStack.length - 1]
      : 0;
    let op = opsList[matchIndex];
    const opStart = op.match(/[,\s]/);
    if (opStart) {
      op = op.substring((opStart.index || 0) + 1).trimStart();
    }
    const postOpString =
      matchesOpen.length > 0 ? str.substring(matchesOpen[matchIndex] + 1) : str;

    return { op, postOpString };
  };

  const setAutocompleteText = (
    searchArray: any[],
    hasNameKey: boolean,
    searchString: string,
    closeOperation: boolean = false,
    isIndexAutocomplete: boolean = false
  ) => {
    const fuse = new Fuse(searchArray, {
      keys: hasNameKey ? ['name'] : [],
      minMatchCharLength: 1,
      location: 0,
      distance: 0,
      threshold: 0,
      isCaseSensitive: true
    });
    const result = fuse.search(searchString);
    if (result.length > 0) {
      const r = result[0].item as any;
      const restOfText = hasNameKey
        ? r.name.substring(searchString.length)
        : r.substring(searchString.length);
      setAutocomplete(
        isIndexAutocomplete
          ? `${restOfText}]`
          : closeOperation
          ? `${restOfText})`
          : restOfText
      );
    } else {
      setAutocomplete('');
    }
  };

  const onKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    const isPQL = queryType === 'PQL';

    if (event.key === 'Enter' && !event.shiftKey) {
      onQueryEnter(event);
      setClearedAfterQuery(false);
    } else if (
      (event.key === 'Tab' || event.key === 'ArrowRight') &&
      showAutocomplete &&
      autocomplete
    ) {
      event.preventDefault();
      ref.current.value = ref.current.value + autocomplete;
      setShowError(false);
      updateHelper(ref.current.value);
      if (isPQL) {
        const pqlPattern = ref.current.value.match(/\[([\w-]*?)\]/);
        if (pqlPattern) {
          onSetIndex(pqlPattern[1]);
        }
        setAutocomplete('');
      } else {
        setAutocomplete('');
      }
    } else if (event.key === 'ArrowUp' && previousQueries.length > 0) {
      if (ref.current.value === '' || queryHistory >= 0) {
        const prevQueryIndex = Math.min(
          queryHistory + 1,
          previousQueries.length - 1
        );
        setQueryHistory(prevQueryIndex);
        if (previousQueries[prevQueryIndex][0] === '!') {
          const qString = previousQueries[prevQueryIndex].substring(1);
          getQueryType(qString);
          ref.current.value = qString;
        } else {
          getQueryType(previousQueries[prevQueryIndex]);
          ref.current.value = previousQueries[prevQueryIndex];
        }
        setTimeout(() => {
          ref.current.selectionStart = ref.current.selectionEnd =
            ref.current.value.length;
        }, 0);
        setInputIsDirty(true);
      }
    } else if (
      event.key === 'ArrowDown' &&
      queryHistory >= 0 &&
      previousQueries.length > 0
    ) {
      const prevQueryIndex = Math.max(queryHistory - 1, -1);
      setQueryHistory(prevQueryIndex);
      if (prevQueryIndex >= 0) {
        if (previousQueries[prevQueryIndex][0] === '!') {
          const qString = previousQueries[prevQueryIndex].substring(1);
          getQueryType(qString);
          ref.current.value = qString;
        } else {
          getQueryType(previousQueries[prevQueryIndex]);
          ref.current.value = previousQueries[prevQueryIndex];
        }
        setInputIsDirty(true);
      } else {
        ref.current.value = '';
        setQueryType('SQL');
        setInputIsDirty(false);
      }
    }
  };

  const onKeyUp = () => {
    const isSelectionRange =
      ref.current?.selectionStart !== ref.current?.selectionEnd;
    const cursorIsAtEnd =
      ref.current.value.length === ref.current?.selectionStart &&
      !isSelectionRange;

    if (cursorIsAtEnd) {
      setShowAutocomplete(true);
    } else {
      setShowAutocomplete(false);
    }
  };

  const updateHelper = (value: string) => {
    if (queryType === 'PQL') {
      const queryString = value.substring(value.indexOf(']') + 1);
      const { op } = getOperation(queryString);
      if (keys.includes(op)) {
        onHelperTextChange(keywordHelpers[op]);
      } else {
        onHelperTextChange([]);
      }
    } else {
      let matchedKeywords: string[] = [];
      for (const key of sqlKeywords) {
        if (value.toLowerCase().trim().includes(key.trim())) {
          matchedKeywords.push(key.trim());
        }
      }

      const currentKeyword =
        matchedKeywords.length > 0
          ? matchedKeywords[matchedKeywords.length - 1]
          : undefined;

      if (currentKeyword && keys.includes(currentKeyword)) {
        onHelperTextChange(keywordHelpers[currentKeyword]);
      } else {
        onHelperTextChange([]);
      }
    }
  };

  const getQueryType = (queryString: string) => {
    if (queryString[0] === '[') {
      setQueryType('PQL');
    } else {
      setQueryType('SQL');
    }
  };

  return (
    <Fragment>
      <Block
        className={classNames(css.inputLayout, {
          [css.active]: focused
        })}
      >
        <div className={css.console}>
          <form className={css.queryConsole} onSubmit={onQueryEnter}>
            <ChevronRightIcon className={css.cursor} color="primary" />
            <div className={css.input}>
              <InputBase
                className={css.textarea}
                placeholder="Enter a query"
                ref={inputEl}
                inputRef={ref}
                onChange={onInputChange}
                onKeyDown={onKeyDown}
                onKeyUp={onKeyUp}
                onBlur={() => {
                  setFocused(false);
                  setShowAutocomplete(false);
                }}
                onFocus={() => {
                  setFocused(true);
                  setShowAutocomplete(true);
                }}
                disabled={loading}
                autoFocus
                multiline
              />
              {autocomplete && showAutocomplete ? (
                <InputBase
                  className={css.autocompleteText}
                  value={`${inputVal}${autocomplete}`}
                  disabled={true}
                  multiline
                />
              ) : null}
            </div>
            {inputIsDirty && (
              <Fragment>
                <Tooltip
                  title={
                    <Typography variant="caption" component="div">
                      It looks like you're running a {queryType} query.
                    </Typography>
                  }
                  placement="top"
                  arrow
                >
                  <div className={css.queryTypeTag}>{queryType}</div>
                </Tooltip>
                <IconButton
                  className={css.iconButton}
                  onClick={clearInput}
                  aria-label="Clear Input"
                >
                  <ClearIcon />
                </IconButton>
              </Fragment>
            )}
            <IconButton
              className={css.iconButton}
              disabled={!inputIsDirty}
              color={inputIsDirty ? 'primary' : 'default'}
              type="submit"
              aria-label="Query"
            >
              <SubdirectoryIcon />
            </IconButton>
          </form>
        </div>
      </Block>
      {showError ? (
        <div className={css.consoleError}>{error?.error}</div>
      ) : null}
    </Fragment>
  );
});
