/* tslint:disable */
import { createTheme } from '@material-ui/core/styles';
import { baseTheme } from 'theme/';

export const darkTheme = createTheme({
  ...baseTheme,
  palette: {
    background: {
      default: '#252a2d',
      paper: '#2e3438'
    },
    primary: {
      contrastText: '#f9f9f9',
      main: '#48b5cd'
    },
    secondary: {
      contrastText: '#f9f9f9',
      main: '#48b5cd'
    },
    error: {
      contrastText: '#f9f9f9',
      main: '#cd6048'
    },
    type: 'dark'
  },
  overrides: {
    ...baseTheme.overrides,
    MuiAppBar: {
      colorDefault: {
        backgroundColor: '#1c2022',
        borderBottom: '1px solid #1c2022'
      }
    },
    MuiButton: {
      contained: {
        color: 'rgba(255, 255, 255, 0.87)',
        backgroundColor: '#3c4449',
        boxShadow: 'none',
        '&:hover': {
          backgroundColor: '#2e3438',
          boxShadow: 'none'
        }
      }
    },
    MuiDialog: {
      paper: {
        backgroundColor: 'var(--default)'
      }
    },
    MuiFormControl: {
      root: {
        background: 'rgba(0, 0, 0, 0.1)'
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
    MuiTooltip: {
      tooltip: {
        backgroundColor: '#1c2022',
      },
      arrow: {
        color: '#1c2022',
      }
    },
    MuiPopover: {
      paper: {
        backgroundColor: '#1c2022',
        padding: 16
      }
    }
  }
});
/* tslint:enable */
