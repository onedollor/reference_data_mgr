import React, { useState } from 'react';
import {
  Box,
  Typography,
  Grid,
  Paper,
  Chip,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Collapse,
  Button
} from '@mui/material';
import {
  ExpandMore as ExpandMoreIcon,
  ExpandLess as ExpandLessIcon,
  Info as InfoIcon
} from '@mui/icons-material';

const ConfigurationPanel = ({ config }) => {
  const [showDetails, setShowDetails] = useState(false);

  if (!config) return null;

  const formatFileSize = (bytes) => {
    return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
  };

  const renderDelimiterOptions = (title, options) => {
    return (
      <Box sx={{ mb: 2 }}>
        <Typography variant="subtitle2" gutterBottom>
          {title}
        </Typography>
        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
          {options.map((option, index) => (
            <Chip
              key={index}
              label={
                option === '\r' ? '\\r' : 
                option === '\n' ? '\\n' : 
                option === '\r\n' ? '\\r\\n' :
                option === '|""\r\n' ? '|""\\r\\n' :
                option
              }
              variant="outlined"
              size="small"
            />
          ))}
        </Box>
      </Box>
    );
  };

  return (
    <Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
        <Typography variant="h6">
          System Configuration
        </Typography>
        
        <Button
          variant="outlined"
          startIcon={showDetails ? <ExpandLessIcon /> : <ExpandMoreIcon />}
          onClick={() => setShowDetails(!showDetails)}
          size="small"
        >
          {showDetails ? 'Hide Details' : 'Show Details'}
        </Button>
      </Box>

      {/* Basic Configuration Summary */}
      <Grid container spacing={2} sx={{ mb: 2 }}>
        <Grid item xs={12} md={6}>
          <Paper variant="outlined" sx={{ p: 2 }}>
            <Typography variant="subtitle2" color="primary" gutterBottom>
              File Limits
            </Typography>
            <Typography variant="body2">
              <strong>Max Upload Size:</strong> {formatFileSize(config.max_upload_size)}
            </Typography>
            <Typography variant="body2">
              <strong>Supported Formats:</strong> {config.supported_formats?.join(', ') || 'CSV'}
            </Typography>
          </Paper>
        </Grid>
        
        <Grid item xs={12} md={6}>
          <Paper variant="outlined" sx={{ p: 2 }}>
            <Typography variant="subtitle2" color="primary" gutterBottom>
              Default CSV Format
            </Typography>
            <Typography variant="body2">
              <strong>Column Delimiter:</strong> <code>{config.default_delimiters?.column_delimiter}</code>
            </Typography>
            <Typography variant="body2">
              <strong>Text Qualifier:</strong> <code>{config.default_delimiters?.text_qualifier}</code>
            </Typography>
            <Typography variant="body2">
              <strong>Row Delimiter:</strong> <code>
                {config.default_delimiters?.row_delimiter === '|""\r\n' ? '|""\\r\\n' : 
                 config.default_delimiters?.row_delimiter}
              </code>
            </Typography>
          </Paper>
        </Grid>
      </Grid>

      {/* Detailed Configuration */}
      <Collapse in={showDetails} timeout="auto" unmountOnExit>
        <Paper variant="outlined" sx={{ p: 3 }}>
          <Typography variant="h6" gutterBottom>
            Detailed Configuration
          </Typography>

          {/* Delimiter Options */}
          <Box sx={{ mb: 3 }}>
            <Typography variant="subtitle1" gutterBottom>
              Available Delimiter Options
            </Typography>
            
            <Grid container spacing={2}>
              <Grid item xs={12} md={6}>
                {renderDelimiterOptions(
                  'Header Delimiters', 
                  config.delimiter_options?.header_delimiter || []
                )}
                {renderDelimiterOptions(
                  'Column Delimiters', 
                  config.delimiter_options?.column_delimiter || []
                )}
              </Grid>
              <Grid item xs={12} md={6}>
                {renderDelimiterOptions(
                  'Row Delimiters', 
                  config.delimiter_options?.row_delimiter || []
                )}
                {renderDelimiterOptions(
                  'Text Qualifiers', 
                  config.delimiter_options?.text_qualifier || []
                )}
              </Grid>
            </Grid>
          </Box>

          {/* System Information */}
          <Box>
            <Typography variant="subtitle1" gutterBottom>
              System Information
            </Typography>
            
            <TableContainer component={Paper} variant="outlined">
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell><strong>Setting</strong></TableCell>
                    <TableCell><strong>Value</strong></TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  <TableRow>
                    <TableCell>Maximum Upload Size</TableCell>
                    <TableCell>{formatFileSize(config.max_upload_size)} ({config.max_upload_size.toLocaleString()} bytes)</TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell>Supported File Formats</TableCell>
                    <TableCell>{config.supported_formats?.join(', ') || 'CSV'}</TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell>Default Header Delimiter</TableCell>
                    <TableCell><code>{config.default_delimiters?.header_delimiter}</code></TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell>Default Column Delimiter</TableCell>
                    <TableCell><code>{config.default_delimiters?.column_delimiter}</code></TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell>Default Row Delimiter</TableCell>
                    <TableCell>
                      <code>
                        {config.default_delimiters?.row_delimiter === '|""\r\n' ? '|""\\r\\n' : 
                         config.default_delimiters?.row_delimiter}
                      </code>
                    </TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell>Default Text Qualifier</TableCell>
                    <TableCell><code>{config.default_delimiters?.text_qualifier}</code></TableCell>
                  </TableRow>
                </TableBody>
              </Table>
            </TableContainer>
          </Box>

          <Box sx={{ mt: 2, display: 'flex', alignItems: 'center', gap: 1 }}>
            <InfoIcon color="info" fontSize="small" />
            <Typography variant="body2" color="text.secondary">
              These settings define the system capabilities and default values for CSV processing.
              Custom values can be specified during file upload.
            </Typography>
          </Box>
        </Paper>
      </Collapse>
    </Box>
  );
};

export default ConfigurationPanel;