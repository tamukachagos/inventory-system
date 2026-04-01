import { createTheme } from '@mui/material/styles';

const buildPalette = (mode) => {
  const isDark = mode === 'dark';
  return {
    mode,
    primary: {
      main: '#8A0F2D',
      light: '#B41E3A',
      dark: '#6A0C22',
      contrastText: '#FFFFFF',
    },
    secondary: {
      main: '#1C2E4A',
    },
    success: {
      main: '#159E66',
    },
    warning: {
      main: '#E68A00',
    },
    error: {
      main: '#D43D51',
    },
    background: {
      default: isDark ? '#0E1728' : '#F8F6F7',
      paper: isDark ? '#132038' : '#FFFFFF',
    },
    text: {
      primary: isDark ? '#F6F8FD' : '#1A2438',
      secondary: isDark ? '#B8C7DF' : '#53617A',
    },
  };
};

const getAppTheme = (mode = 'light') => createTheme({
  spacing: 8,
  shape: {
    borderRadius: 12,
  },
  palette: buildPalette(mode),
  typography: {
    fontFamily: '"Aptos","Segoe UI","Trebuchet MS","Helvetica Neue",Arial,sans-serif',
    h3: { fontWeight: 700, fontSize: '2rem', lineHeight: 1.2 },
    h4: { fontWeight: 700, fontSize: '1.6rem', lineHeight: 1.25 },
    h5: { fontWeight: 700, fontSize: '1.3rem', lineHeight: 1.3 },
    h6: { fontWeight: 700, fontSize: '1.05rem', lineHeight: 1.35 },
    subtitle1: { fontWeight: 600, fontSize: '0.95rem' },
    body1: { fontSize: '0.95rem' },
    body2: { fontSize: '0.85rem' },
    button: { fontWeight: 700, textTransform: 'none' },
  },
  components: {
    MuiCssBaseline: {
      styleOverrides: {
        ':focus-visible': {
          outline: '2px solid #0F5FFF',
          outlineOffset: 2,
        },
      },
    },
    MuiCard: {
      styleOverrides: {
        root: ({ theme }) => ({
          border: `1px solid ${theme.palette.divider}`,
          boxShadow: theme.palette.mode === 'dark'
            ? '0 8px 24px rgba(2,8,24,0.35)'
            : '0 8px 24px rgba(17,33,62,0.08)',
          backdropFilter: 'blur(8px)',
          transition: 'transform 180ms ease, box-shadow 180ms ease',
          '&:hover': {
            transform: 'translateY(-2px)',
            boxShadow: theme.palette.mode === 'dark'
              ? '0 12px 28px rgba(2,8,24,0.45)'
              : '0 12px 28px rgba(17,33,62,0.14)',
          },
        }),
      },
    },
    MuiButton: {
      styleOverrides: {
        root: {
          borderRadius: 10,
          paddingTop: 8,
          paddingBottom: 8,
          transition: 'transform 120ms ease, box-shadow 150ms ease, background-color 150ms ease',
          '&:active': {
            transform: 'translateY(1px) scale(0.99)',
          },
        },
      },
    },
    MuiListItemButton: {
      styleOverrides: {
        root: ({ theme }) => ({
          transition: 'background-color 150ms ease, transform 120ms ease',
          '&:hover': {
            backgroundColor: theme.palette.action.hover,
            transform: 'translateX(2px)',
          },
        }),
      },
    },
    MuiDialog: {
      defaultProps: {
        transitionDuration: 180,
      },
    },
    MuiSnackbar: {
      defaultProps: {
        anchorOrigin: { vertical: 'bottom', horizontal: 'right' },
      },
    },
  },
});

export default getAppTheme;
