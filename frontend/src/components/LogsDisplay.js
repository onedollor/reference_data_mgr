import React, { useState, useEffect, useCallback } from 'react';
import {
  Box,
  Typography,
  Button,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Chip,
  Collapse,
  IconButton,
  Alert
} from '@mui/material';
import {
  Refresh as RefreshIcon,
  ExpandMore as ExpandMoreIcon,
  ExpandLess as ExpandLessIcon
} from '@mui/icons-material';

const LogsDisplay = () => {
  const [logs, setLogs] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [expandedRows, setExpandedRows] = useState(new Set());
  const [lastUpdated, setLastUpdated] = useState(null);
  const [expandedList, setExpandedList] = useState(false); // controls 10 vs 100 rows

  const loadLogs = useCallback(async () => {
    setLoading(true);
    setError('');
    
    try {
      // Add cache busting via no-store and timestamp param
      const ts = Date.now();
      const response = await fetch(`/logs?ts=${ts}`, {
        cache: 'no-store',
        headers: {
          'Cache-Control': 'no-cache',
          'Pragma': 'no-cache'
        }
      });
      
      if (response.ok) {
        const result = await response.json();
        setLogs(result.logs || []);
        setLastUpdated(new Date());
      } else {
        throw new Error('Failed to load logs');
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadLogs();
    const interval = setInterval(loadLogs, 10000);
    return () => clearInterval(interval);
  }, [loadLogs]);

  const toggleRowExpansion = (index) => {
    const newExpanded = new Set(expandedRows);
    if (newExpanded.has(index)) {
      newExpanded.delete(index);
    } else {
      newExpanded.add(index);
    }
    setExpandedRows(newExpanded);
  };

  const getLevelColor = (level) => {
    switch (level?.toUpperCase()) {
      case 'ERROR':
        return 'error';
      case 'WARNING':
        return 'warning';
      case 'INFO':
        return 'info';
      case 'DEBUG':
        return 'default';
      default:
        return 'default';
    }
  };

  const formatTimestampPair = (log) => {
    const { timestamp, timestamp_local, tz } = log;
    let localDisp = 'N/A';
    let utcDisp = 'N/A';
    try { if (timestamp_local) localDisp = new Date(timestamp_local).toLocaleString(); } catch {}
    try { if (timestamp) utcDisp = new Date(timestamp).toLocaleString(undefined, { timeZone: 'UTC' }); } catch {}
    return { localDisp, utcDisp, tz };
  };

  const formatMessage = (message) => {
    if (!message) return '';
    
    // Truncate long messages for table display
    if (message.length > 100) {
      return message.substring(0, 100) + '...';
    }
    
    return message;
  };

  const renderExpandedContent = (log) => {
    return (
      <Box sx={{ p: 2, backgroundColor: '#f8f9fa' }}>
        {/* Full Message */}
        <Typography variant="subtitle2" gutterBottom>
          Full Message:
        </Typography>
        <Typography 
          variant="body2" 
          sx={{ 
            fontFamily: 'monospace',
            backgroundColor: 'white',
            p: 1,
            borderRadius: 1,
            border: '1px solid #e0e0e0',
            mb: 2,
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-word'
          }}
        >
          {log.message}
        </Typography>

        {/* Traceback */}
        {log.traceback && (
          <>
            <Typography variant="subtitle2" gutterBottom color="error">
              Traceback:
            </Typography>
            <Typography 
              variant="body2" 
              sx={{ 
                fontFamily: 'monospace',
                backgroundColor: '#ffebee',
                p: 1,
                borderRadius: 1,
                border: '1px solid #ffcdd2',
                mb: 2,
                whiteSpace: 'pre-wrap',
                wordBreak: 'break-word',
                color: '#d32f2f'
              }}
            >
              {log.traceback}
            </Typography>
          </>
        )}

        {/* Metadata */}
        {log.metadata && Object.keys(log.metadata).length > 0 && (
          <>
            <Typography variant="subtitle2" gutterBottom>
              Additional Information:
            </Typography>
            <Box sx={{ 
              backgroundColor: 'white',
              p: 1,
              borderRadius: 1,
              border: '1px solid #e0e0e0'
            }}>
              {Object.entries(log.metadata).map(([key, value]) => (
                value && (
                  <Typography key={key} variant="body2" sx={{ mb: 0.5 }}>
                    <strong>{key}:</strong> {String(value)}
                  </Typography>
                )
              ))}
            </Box>
          </>
        )}
      </Box>
    );
  };

  return (
    <Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
        <Typography variant="h6" sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          System Logs
        </Typography>
        <Box sx={{ display: 'flex', gap: 1 }}>
          {logs.length > 10 && (
            <Button
              variant="outlined"
              size="small"
              onClick={() => setExpandedList(e => !e)}
            >
              {expandedList ? 'Show 10' : 'Show 100'}
            </Button>
          )}
          <Button
            variant="outlined"
            startIcon={<RefreshIcon />}
            onClick={loadLogs}
            disabled={loading}
            size="small"
          >
            {loading ? 'Loading...' : 'Refresh'}
          </Button>
        </Box>
      </Box>
      {logs.length > 0 && (
        <Typography variant="caption" sx={{ display: 'block', mb: 1, color: 'text.secondary' }}>
          {expandedList
            ? `Showing last ${Math.min(100, logs.length)} log entries`
            : `Showing last ${Math.min(100, logs.length)} log entries (displaying ${Math.min(10, logs.length)} – click 'Show 100' to expand)`}
        </Typography>
      )}

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      {logs.length === 0 && !loading && (
        <Alert severity="info">
          No logs available
        </Alert>
      )}

      {logs.length > 0 && (
        <TableContainer component={Paper} variant="outlined">
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell width={50}></TableCell>
                <TableCell>Timestamp</TableCell>
                <TableCell>Level</TableCell>
                <TableCell>Action</TableCell>
                <TableCell>Message</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {(() => {
                const limit = expandedList ? 100 : 10;
                const displayLogs = logs.slice(-limit); // last N logs
                return displayLogs.map((log, idx) => (
                  <React.Fragment key={idx}>
                    <TableRow hover>
                      <TableCell>
                        <IconButton
                          size="small"
                          onClick={() => toggleRowExpansion(idx)}
                        >
                          {expandedRows.has(idx) ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                        </IconButton>
                      </TableCell>
                      <TableCell>
                        {(() => {
                          const t = formatTimestampPair(log);
                          return (
                            <Typography variant="body2" sx={{ fontFamily: 'monospace', fontSize: '0.65rem' }}>
                              {t.localDisp}{t.tz ? ` (${t.tz})` : ''}
                              <br />
                              <span style={{ color: '#666' }}>UTC: {t.utcDisp}</span>
                            </Typography>
                          );
                        })()}
                      </TableCell>
                      <TableCell>
                        <Chip
                          label={log.level}
                          color={getLevelColor(log.level)}
                          size="small"
                          variant="outlined"
                        />
                      </TableCell>
                      <TableCell>
                        <Typography variant="body2">
                          {log.action_step}
                        </Typography>
                      </TableCell>
                      <TableCell>
                        <Typography
                          variant="body2"
                          sx={{
                            color: log.level === 'ERROR' ? '#d32f2f' : 'inherit',
                            fontWeight: log.level === 'ERROR' ? 'bold' : 'normal'
                          }}
                        >
                          {formatMessage(log.message)}
                        </Typography>
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={5}>
                        <Collapse in={expandedRows.has(idx)} timeout="auto" unmountOnExit>
                          {renderExpandedContent(log)}
                        </Collapse>
                      </TableCell>
                    </TableRow>
                  </React.Fragment>
                ));
              })()}
            </TableBody>
          </Table>
        </TableContainer>
      )}

      <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>
        Showing last {logs.length} log entries. Auto-refresh every 10 seconds.
        {lastUpdated && (
          <> Last updated: {lastUpdated.toLocaleTimeString()}.</>
        )}
      </Typography>
    </Box>
  );
};

export default LogsDisplay;