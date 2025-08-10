import React from 'react';
import ReactDOM from 'react-dom/client';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';
import App from './App';

// Create TD-themed custom theme
const theme = createTheme({
  palette: {
    primary: {
      main: '#54B848', // TD Brand Green
      light: '#7BC76F',
      dark: '#3A9B2F',
      contrastText: '#ffffff',
    },
    secondary: {
      main: '#003F2D', // TD British Racing Green
      light: '#2C5F4A',
      dark: '#002B1F',
      contrastText: '#ffffff',
    },
    success: {
      main: '#54B848', // Use TD green for success states
    },
    background: {
      default: '#f8f9fa', // Clean light background
      paper: '#ffffff',
    },
    text: {
      primary: '#2c2c2c',
      secondary: '#666666',
    },
  },
  typography: {
    fontFamily: '"Helvetica Neue", "Arial", sans-serif', // Clean, professional font
    h3: {
      fontWeight: 600,
      color: '#2c2c2c',
    },
    h4: {
      fontWeight: 600,
      color: '#2c2c2c',
    },
    h5: {
      fontWeight: 500,
      color: '#2c2c2c',
    },
    h6: {
      fontWeight: 500,
      color: '#2c2c2c',
    },
    button: {
      fontWeight: 600,
      textTransform: 'none', // TD style - no uppercase transform
    },
  },
  shape: {
    borderRadius: 8, // TD style - moderate rounded corners
  },
  components: {
    MuiPaper: {
      styleOverrides: {
        root: {
          backgroundColor: '#ffffff',
          boxShadow: '0 2px 8px rgba(0,0,0,0.1)', // Subtle TD-style shadow
        },
      },
    },
    MuiButton: {
      styleOverrides: {
        root: {
          borderRadius: 8,
          padding: '10px 24px',
          fontWeight: 600,
        },
        contained: {
          boxShadow: '0 2px 4px rgba(0,0,0,0.2)',
          '&:hover': {
            boxShadow: '0 4px 8px rgba(0,0,0,0.25)',
          },
        },
      },
    },
    MuiCard: {
      styleOverrides: {
        root: {
          borderRadius: 12,
          boxShadow: '0 2px 12px rgba(0,0,0,0.08)',
          border: '1px solid #e0e0e0',
        },
      },
    },
    MuiLinearProgress: {
      styleOverrides: {
        root: {
          borderRadius: 4,
          height: 8,
        },
      },
    },
  },
});

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(
  <React.StrictMode>
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <App />
    </ThemeProvider>
  </React.StrictMode>
);