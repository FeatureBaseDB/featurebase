/* tslint:disable */
import { createMuiTheme } from '@material-ui/core/styles';
import { baseTheme } from 'theme/';

export const lightTheme = createMuiTheme({
  ...baseTheme,
  palette: {
    background: {
      default: '#f5f7f9'
    },
    primary: {
      contrastText: '#f9f9f9',
      main: '#48b5cd'
    },
    secondary: {
      contrastText: '#f9f9f9',
      main: '#48b5cd'
    }
  },
  overrides: {
    ...baseTheme.overrides,
    MuiAppBar: {
      colorDefault: {
        backgroundColor: '#ffffff',
        borderBottom: '1px solid rgba(0, 0, 0, 0.12)'
      }
    },
    MuiButton: {
      contained: {
        backgroundColor: 'var(--surface)',
        boxShadow:
          '0px 3px 1px -2px rgba(0,0,0,0.1), 0px 2px 2px 0px rgba(0,0,0,0.07), 0px 1px 5px 0px rgba(0,0,0,0.06)',
        '&:hover': {
          backgroundColor: '#e9edf2',
          boxShadow:
            '0px 3px 1px -2px rgba(0,0,0,0.1), 0px 2px 2px 0px rgba(0,0,0,0.07), 0px 1px 5px 0px rgba(0,0,0,0.06)'
        }
      }
    },
    MuiInputBase: {
      root: {
        background: 'rgba(var(--default-rgb), 0.1)'
      }
    },
    MuiOutlinedInput: {
      root: {
        '&$focused $notchedOutline': {
          borderColor: '#66b4cb'
        }
      }
    },
    MuiInputLabel: {
      root: {
        '&$focused': {
          color: '#66b4cb'
        }
      }
    },
    MuiListItem: {
      button: {
        '&:hover': {
          backgroundColor: 'rgba(var(--secondary-rgb), 0.1)'
        }
      }
    },
    MuiTableRow: {
      root: {
        '&$hover:hover': {
          backgroundColor: 'rgba(var(--secondary-rgb), 0.1)'
        }
      }
    }
  }
});
/* tslint:enable */
