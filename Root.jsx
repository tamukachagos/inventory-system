import { useMemo, useState } from 'react';
import { ThemeProvider } from '@mui/material/styles';
import App from './App.jsx';
import getAppTheme from './theme.js';

const getInitialMode = () => {
  const stored = localStorage.getItem('inventory_theme_mode');
  if (stored === 'light' || stored === 'dark') return stored;
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
};

function Root() {
  const [themeMode, setThemeMode] = useState(getInitialMode);
  const theme = useMemo(() => getAppTheme(themeMode), [themeMode]);

  const toggleThemeMode = () => {
    setThemeMode((prev) => {
      const next = prev === 'dark' ? 'light' : 'dark';
      localStorage.setItem('inventory_theme_mode', next);
      return next;
    });
  };

  return (
    <ThemeProvider theme={theme}>
      <App colorMode={themeMode} onToggleColorMode={toggleThemeMode} />
    </ThemeProvider>
  );
}

export default Root;
