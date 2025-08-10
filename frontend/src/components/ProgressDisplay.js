import React from 'react';
import {
  Box,
  Typography,
  LinearProgress,
  Paper,
  Button
} from '@mui/material';

const ProgressDisplay = ({ uploadProgress, ingestionData, progressKey, onCancel, onReset }) => {
  
  const setFormattedMessage = (message) => {
    if (!message) return null;
    
    // Split message into lines
    const lines = message.split('\n');
    
    return lines.map((line, index) => {
      // Check if line contains "ERROR!"
      const isErrorLine = line.includes('ERROR!');
      
      // Split long lines (over 80 characters)
      if (line.length > 80) {
        const chunks = [];
        for (let i = 0; i < line.length; i += 80) {
          chunks.push(line.substring(i, i + 80));
        }
        
        return chunks.map((chunk, chunkIndex) => (
          <span 
            key={`${index}-${chunkIndex}`}
            className={`progress-line ${isErrorLine ? 'error-line' : ''}`}
            style={{
              display: 'block',
              color: isErrorLine ? '#d32f2f' : 'inherit',
              fontWeight: isErrorLine ? 'bold' : 'normal',
              fontFamily: 'monospace',
              fontSize: '0.875rem',
              margin: '2px 0',
              wordBreak: 'break-all'
            }}
          >
            {chunk}
            {chunkIndex < chunks.length - 1 && <br />}
          </span>
        ));
      } else {
        return (
          <span 
            key={index}
            className={`progress-line ${isErrorLine ? 'error-line' : ''}`}
            style={{
              display: 'block',
              color: isErrorLine ? '#d32f2f' : 'inherit',
              fontWeight: isErrorLine ? 'bold' : 'normal',
              fontFamily: 'monospace',
              fontSize: '0.875rem',
              margin: '2px 0',
              wordBreak: 'break-word'
            }}
          >
            {line}
          </span>
        );
      }
    });
  };

  return (
    <Box>
      {/* Upload Progress */}
      {uploadProgress !== null && (
        <Box sx={{ mb: 3 }}>
          <Typography variant="subtitle2" gutterBottom>
            Upload Progress
          </Typography>
          <LinearProgress 
            variant="determinate" 
            value={uploadProgress} 
            sx={{ mb: 1 }}
          />
          <Typography variant="body2" color="text.secondary">
            {uploadProgress}% complete
          </Typography>
        </Box>
      )}

      {/* Ingestion Progress */}
      {ingestionData && (
        <Box>
          <Box sx={{ display:'flex', justifyContent:'space-between', alignItems:'center' }}>
            <Typography variant="subtitle2" gutterBottom>
              Data Ingestion Progress
            </Typography>
            <Box sx={{ display: 'flex', gap: 1 }}>
              {!ingestionData.done && !ingestionData.canceled && (
                <Button size="small" color="warning" onClick={onCancel} disabled={!progressKey}>
                  Cancel
                </Button>
              )}
              {(ingestionData.error || ingestionData.canceled) && onReset && (
                <Button size="small" color="primary" onClick={onReset}>
                  Cancel Current Upload
                </Button>
              )}
              {ingestionData.done && !ingestionData.error && !ingestionData.canceled && onReset && (
                <Button size="small" color="success" onClick={onReset}>
                  Start New Upload
                </Button>
              )}
            </Box>
          </Box>
          <LinearProgress
            variant={typeof ingestionData.percent === 'number' && ingestionData.total ? 'determinate' : 'indeterminate'}
            value={ingestionData.total ? ingestionData.percent : undefined}
            sx={{ mb: 1 }}
            color={ingestionData.error ? 'error' : (ingestionData.done ? 'success' : 'primary')}
          />
          <Typography variant="body2" sx={{ mb: 1 }}>
            {ingestionData.canceled ? 'Canceled by user' : ingestionData.error ? `Error: ${ingestionData.error}` : (
              ingestionData.done ? 'Completed' : `Stage: ${ingestionData.stage}${
                ingestionData.stage === 'inserting' && ingestionData.total ? 
                  ` • ${ingestionData.percent}% (${ingestionData.inserted?.toLocaleString()}/${ingestionData.total.toLocaleString()} rows)` :
                  ingestionData.total ? ` • ${ingestionData.percent}%` : ''
              }`
            )}
          </Typography>
          {ingestionData.total && ingestionData.inserted !== undefined && (
            <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 1 }}>
              Rows: {ingestionData.inserted.toLocaleString()}/{ingestionData.total.toLocaleString()}
            </Typography>
          )}
          {ingestionData.error && (
            <Paper variant="outlined" sx={{ p:1, background:'#ffeaea', borderColor:'#f44336' }}>
              <Typography variant="caption" color="error.main">
                Ingestion failed. Check logs for details.
              </Typography>
            </Paper>
          )}
        </Box>
      )}
    </Box>
  );
};

export default ProgressDisplay;