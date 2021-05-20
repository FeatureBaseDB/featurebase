export const baseTheme = {
  typography: {
    h1: {
      fontFamily: "'Open Sans', Roboto, sans-serif",
      fontSize: '4.25rem',
      fontWeight: 300
    },
    h2: {
      fontFamily: "'Open Sans', Roboto, sans-serif",
      fontWeight: 300
    },
    h3: {
      fontFamily: "'Open Sans', Roboto, sans-serif",
      fontWeight: 300
    },
    h4: {
      fontFamily: "'Open Sans', Roboto, sans-serif",
      fontWeight: 300
    },
    h5: {
      fontFamily: "'Open Sans', Roboto, sans-serif",
      fontWeight: 300
    },
    h6: {
      fontFamily: "'Open Sans', Roboto, sans-serif",
      fontWeight: 400
    }
  },
  overrides: {
    MuiBackdrop: {
      root: {
        backgroundColor: 'rgba(0, 0, 0, 0.8)'
      }
    },
    MuiButton: {
      root: {
        borderRadius: 2
      },
      text: {
        padding: '6px 16px' // matching contained button padding
      }
    },
    MuiBreadcrumbs: {
      root: {
        paddingBottom: '32px'
      },
      separator: {
        fontSize: '0.875rem',
        lineHeight: '1.43'
      }
    },
    MuiInputBase: {
      inputMarginDense: {
        fontSize: '14px'
      }
    },
    MuiInputLabel: {
      outlined: {
        zIndex: 'auto'
      }
    },
    MuiTableRow: {
      hover: {
        cursor: 'pointer'
      }
    },
    MuiTableCell: {
      sizeSmall: {
        padding: '0 8px'
      }
    },
    MuiTooltip: {
      tooltip: {
        maxWidth: 500
      }
    },
    MuiIconButton: {
      sizeSmall: {
        padding: 5
      }
    },
    MuiCardActions: {
      root: {
        padding: '8px 16px'
      }
    },
    MuiChip: {
      sizeSmall: {
        fontSize: '0.75rem',
        height: 20,
        lineHeight: 1,
        borderRadius: 12
      }
    }
  }
};
