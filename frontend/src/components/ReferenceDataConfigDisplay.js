import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Alert,
  CircularProgress,
  Chip,
  IconButton,
  Tooltip
} from '@mui/material';
import RefreshIcon from '@mui/icons-material/Refresh';

function ReferenceDataConfigDisplay({ refreshTrigger = 0 }) {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastRefresh, setLastRefresh] = useState(null);

  useEffect(() => {
    loadReferenceDataConfig();
  }, []);

  // Auto-refresh when refreshTrigger changes
  useEffect(() => {
    if (refreshTrigger > 0) {
      console.log('Reference Data Config auto-refresh triggered:', refreshTrigger);
      loadReferenceDataConfig();
    }
  }, [refreshTrigger]);

  const loadReferenceDataConfig = async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch('/reference-data-config');
      if (!response.ok) {
        throw new Error(`Failed to load Reference Data Config: ${response.status}`);
      }
      const result = await response.json();
      setData(result.data || []);
      setLastRefresh(new Date());
    } catch (err) {
      setError(err.message);
      console.error('Failed to load Reference Data Config:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleRefresh = () => {
    loadReferenceDataConfig();
  };

  if (loading) {
    return (
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', p: 4 }}>
        <CircularProgress size={24} sx={{ mr: 2, color: 'primary.main' }} />
        <Typography variant="body2" color="text.secondary">
          Loading Reference Data Configuration...
        </Typography>
      </Box>
    );
  }

  if (error) {
    return (
      <Alert severity="error" variant="outlined" sx={{ borderRadius: 2 }}>
        <Typography variant="body2">
          Failed to load Reference Data Configuration: {error}
        </Typography>
      </Alert>
    );
  }

  return (
    <Box>
      {/* Header with refresh button */}
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
        <Typography variant="body2" color="text.secondary">
          {data.length} configuration{data.length !== 1 ? 's' : ''} found
          {lastRefresh && (
            <Typography component="span" variant="caption" sx={{ ml: 2, color: 'text.disabled' }}>
              Last updated: {lastRefresh.toLocaleTimeString()}
            </Typography>
          )}
        </Typography>
        <Tooltip title="Refresh data">
          <IconButton 
            onClick={handleRefresh} 
            size="small" 
            sx={{ color: 'primary.main' }}
          >
            <RefreshIcon />
          </IconButton>
        </Tooltip>
      </Box>

      {data.length === 0 ? (
        <Alert severity="info" variant="outlined" sx={{ borderRadius: 2 }}>
          <Typography variant="body2">
            No Reference Data Configuration records found. Records are automatically created after successful data ingestion.
          </Typography>
        </Alert>
      ) : (
        <TableContainer component={Paper} sx={{ borderRadius: 2, border: '1px solid #e0e0e0' }}>
          <Table size="small">
            <TableHead>
              <TableRow sx={{ backgroundColor: 'primary.main' }}>
                <TableCell sx={{ color: 'white', fontWeight: 600 }}>Stored Procedure</TableCell>
                <TableCell sx={{ color: 'white', fontWeight: 600 }}>Reference Name</TableCell>
                <TableCell sx={{ color: 'white', fontWeight: 600 }}>Source Database</TableCell>
                <TableCell sx={{ color: 'white', fontWeight: 600 }}>Schema</TableCell>
                <TableCell sx={{ color: 'white', fontWeight: 600 }}>Table</TableCell>
                <TableCell sx={{ color: 'white', fontWeight: 600 }} align="center">Status</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {data.map((row, index) => (
                <TableRow 
                  key={row.ref_name || index}
                  sx={{ 
                    '&:nth-of-type(odd)': { backgroundColor: '#f8f9fa' },
                    '&:hover': { backgroundColor: '#e8f5e8' }
                  }}
                >
                  <TableCell>
                    <Typography variant="body2" sx={{ fontFamily: 'monospace', fontSize: '0.875rem' }}>
                      {row.sp_name}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2" sx={{ fontWeight: 500 }}>
                      {row.ref_name}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2">
                      {row.source_db}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>
                      {row.source_schema}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>
                      {row.source_table}
                    </Typography>
                  </TableCell>
                  <TableCell align="center">
                    <Chip
                      label={row.is_enabled === 1 ? 'Enabled' : 'Disabled'}
                      size="small"
                      color={row.is_enabled === 1 ? 'success' : 'default'}
                      variant={row.is_enabled === 1 ? 'filled' : 'outlined'}
                      sx={{ 
                        minWidth: 80,
                        fontWeight: 500,
                        ...(row.is_enabled === 1 && { 
                          backgroundColor: 'primary.main',
                          color: 'white'
                        })
                      }}
                    />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      )}
    </Box>
  );
}

export default ReferenceDataConfigDisplay;