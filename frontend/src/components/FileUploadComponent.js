import React, { useState } from 'react';
import {
  Box,
  Button,
  CircularProgress,
  FormControl,
  FormControlLabel,
  Grid,
  InputLabel,
  MenuItem,
  RadioGroup,
  Radio,
  Select,
  TextField,
  Typography,
  Alert,
  LinearProgress,
  Input
} from '@mui/material';
import CloudUploadIcon from '@mui/icons-material/CloudUpload';
import LoadTypeWarningDialog from './LoadTypeWarningDialog';

const FileUploadComponent = ({ 
  config, 
  onUploadStart, 
  onUploadComplete, 
  onIngestionProgress,
  onProcessingComplete,
  disabled 
}) => {
  const [selectedFile, setSelectedFile] = useState(null);
  const [formatConfig, setFormatConfig] = useState({
    header_delimiter: '|',
    column_delimiter: '|',
    row_delimiter: '|""\r\n',
    text_qualifier: '"',
    skip_lines: 0,
    trailer_line: '',
    load_mode: 'full'
  });
  const [autoDetection, setAutoDetection] = useState({
    isDetecting: false,
    detected: false,
    confidence: 0,
    suggestions: null,
    analysis: null
  });
  const [customValues, setCustomValues] = useState({
    header_delimiter: '',
    column_delimiter: '',
    row_delimiter: '',
    text_qualifier: ''
  });
  const [useCustom, setUseCustom] = useState({
    header_delimiter: false,
    column_delimiter: false,
    row_delimiter: false,
    text_qualifier: false
  });
  const [uploadError, setUploadError] = useState('');
  const [isUploading, setIsUploading] = useState(false);
  const [showLoadTypeDialog, setShowLoadTypeDialog] = useState(false);
  const [loadTypeVerification, setLoadTypeVerification] = useState(null);
  const [pendingUploadData, setPendingUploadData] = useState(null);

  const handleFileSelect = async (event) => {
    const file = event.target.files[0];
    if (file) {
      if (file.size > config.max_upload_size) {
        setUploadError(`File size (${(file.size / 1024 / 1024).toFixed(2)} MB) exceeds maximum limit of ${(config.max_upload_size / 1024 / 1024).toFixed(2)} MB`);
        setSelectedFile(null);
      } else if (!file.name.toLowerCase().endsWith('.csv')) {
        setUploadError('Only CSV files are supported');
        setSelectedFile(null);
      } else {
        setSelectedFile(file);
        setUploadError('');
        
        // Reset auto-detection state
        setAutoDetection({
          isDetecting: false,
          detected: false,
          confidence: 0,
          suggestions: null,
          analysis: null
        });
        
        // Automatically trigger format detection after file selection
        // Use setTimeout to ensure file state is updated first
        setTimeout(async () => {
          try {
            setAutoDetection(prev => ({ ...prev, isDetecting: true }));
            
            // Reset trailer line to empty when starting auto-detection
            setFormatConfig(prev => ({
              ...prev,
              trailer_line: ''
            }));

            const formData = new FormData();
            formData.append('file', file);

            const response = await fetch('/detect-format', {
              method: 'POST',
              body: formData,
            });

            if (!response.ok) {
              throw new Error('Format detection failed');
            }

            const result = await response.json();
            
            const detected = {
              isDetecting: false,
              detected: true,
              confidence: result.confidence,
              suggestions: result.detected_format,
              analysis: result.analysis
            };
            setAutoDetection(detected);
            
            // Immediately apply detected settings (auto-save behavior)
            if (detected.suggestions) {
              setFormatConfig(prev => ({
                ...prev,
                header_delimiter: detected.suggestions.header_delimiter,
                column_delimiter: detected.suggestions.column_delimiter,
                row_delimiter: detected.suggestions.row_delimiter,
                text_qualifier: detected.suggestions.text_qualifier,
                skip_lines: detected.suggestions.skip_lines,
                trailer_line: detected.suggestions.trailer_line || ''
              }));
            }
            
            console.log('Auto-detection completed after file selection');

          } catch (error) {
            console.error('Auto-detection failed:', error);
            setAutoDetection({
              isDetecting: false,
              detected: false,
              confidence: 0,
              suggestions: null,
              analysis: null
            });
          }
        }, 100); // Small delay to ensure state updates
      }
    }
  };

  const handleFormatChange = (field, value) => {
    setFormatConfig(prev => ({
      ...prev,
      [field]: value
    }));
  };

  const handleCustomToggle = (field, isCustom) => {
    setUseCustom(prev => ({
      ...prev,
      [field]: isCustom
    }));
    
    if (!isCustom) {
      // Reset to default when switching away from custom
      setFormatConfig(prev => ({
        ...prev,
        [field]: config.default_delimiters[field]
      }));
    }
  };

  const handleCustomValueChange = (field, value) => {
    setCustomValues(prev => ({
      ...prev,
      [field]: value
    }));
    
    if (useCustom[field]) {
      setFormatConfig(prev => ({
        ...prev,
        [field]: value
      }));
    }
  };

  const detectFormat = async () => {
    if (!selectedFile) return;

    setAutoDetection(prev => ({ ...prev, isDetecting: true }));
    
    // Reset trailer line to empty when starting auto-detection
    setFormatConfig(prev => ({
      ...prev,
      trailer_line: ''
    }));

    try {
      const formData = new FormData();
      formData.append('file', selectedFile);

      const response = await fetch('/detect-format', {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) {
        throw new Error('Format detection failed');
      }

      const result = await response.json();
      
      const detected = {
        isDetecting: false,
        detected: true,
        confidence: result.confidence,
        suggestions: result.detected_format,
        analysis: result.analysis
      };
      setAutoDetection(detected);
      // Immediately apply detected settings (auto-save behavior)
      if (detected.suggestions) {
        setFormatConfig(prev => ({
          ...prev,
          header_delimiter: detected.suggestions.header_delimiter,
          column_delimiter: detected.suggestions.column_delimiter,
          row_delimiter: detected.suggestions.row_delimiter,
          text_qualifier: detected.suggestions.text_qualifier,
          skip_lines: detected.suggestions.skip_lines,
          trailer_line: detected.suggestions.trailer_line || ''
        }));
      }

    } catch (error) {
      console.error('Auto-detection failed:', error);
      setAutoDetection({
        isDetecting: false,
        detected: false,
        confidence: 0,
        suggestions: null,
        analysis: null
      });
    }
  };

  // applyDetectedFormat removed: auto-applied immediately after detection

  const renderDelimiterSelect = (fieldName, label) => {
    const options = config.delimiter_options[fieldName] || [];
    const isCustomSelected = useCustom[fieldName];
    
    return (
      <Grid item xs={12} md={6}>
        <FormControl fullWidth>
          <InputLabel>{label}</InputLabel>
          <Select
            value={isCustomSelected ? 'custom' : formatConfig[fieldName]}
            onChange={(e) => {
              if (e.target.value === 'custom') {
                handleCustomToggle(fieldName, true);
              } else {
                handleCustomToggle(fieldName, false);
                handleFormatChange(fieldName, e.target.value);
              }
            }}
            label={label}
          >
            {options.map((option) => (
              <MenuItem key={option} value={option}>
                {option === '\r' ? '\\r' : 
                 option === '\n' ? '\\n' : 
                 option === '\r\n' ? '\\r\\n' :
                 option === '|""\r\n' ? '|""\\r\\n' :
                 option}
              </MenuItem>
            ))}
            <MenuItem value="custom">Custom</MenuItem>
          </Select>
        </FormControl>
        
        {isCustomSelected && (
          <TextField
            fullWidth
            label={`Custom ${label}`}
            value={customValues[fieldName]}
            onChange={(e) => handleCustomValueChange(fieldName, e.target.value)}
            placeholder={`Enter custom ${label.toLowerCase()}`}
            sx={{ mt: 1 }}
          />
        )}
      </Grid>
    );
  };

  const verifyLoadType = async () => {
    if (!selectedFile) return;

    try {
      const formData = new FormData();
      formData.append('filename', selectedFile.name);
      formData.append('load_mode', formatConfig.load_mode);

      const response = await fetch('/verify-load-type', {
        method: 'POST',
        body: formData,
      });

      if (response.ok) {
        const result = await response.json();
        return result;
      } else {
        console.warn('Load type verification failed, proceeding with upload');
        return null;
      }
    } catch (error) {
      console.warn('Load type verification error:', error);
      return null;
    }
  };

  const performUpload = async (overrideLoadType = null) => {
    const formData = new FormData();
    formData.append('file', selectedFile);
    formData.append('header_delimiter', formatConfig.header_delimiter);
    formData.append('column_delimiter', formatConfig.column_delimiter);
    formData.append('row_delimiter', formatConfig.row_delimiter);
    formData.append('text_qualifier', formatConfig.text_qualifier);
    formData.append('skip_lines', formatConfig.skip_lines.toString());
    formData.append('load_mode', formatConfig.load_mode);
    
    if (formatConfig.trailer_line) {
      formData.append('trailer_line', formatConfig.trailer_line);
    }
    
    if (overrideLoadType) {
      formData.append('override_load_type', overrideLoadType);
    }

    const response = await fetch('/upload', {
      method: 'POST',
      body: formData,
    });

    const result = await response.json();

    if (response.ok) {
      onUploadComplete(result);
    } else {
      throw new Error(result.detail || 'Upload failed');
    }
  };

  const handleUpload = async () => {
    if (!selectedFile) return;

    setIsUploading(true);
    setUploadError('');
    onUploadStart();

    try {
      // First, verify load type compatibility
      const verification = await verifyLoadType();
      
      if (verification && verification.has_mismatch) {
        // Show warning dialog for mismatch
        setLoadTypeVerification(verification);
        setShowLoadTypeDialog(true);
        setIsUploading(false); // Stop loading while waiting for user decision
        return;
      }
      
      // No mismatch or verification failed - proceed with upload
      await performUpload();

    } catch (error) {
      setUploadError(`Upload failed: ${error.message}`);
      onProcessingComplete();
    } finally {
      setIsUploading(false);
    }
  };

  const handleLoadTypeOverride = async (overrideType) => {
    setShowLoadTypeDialog(false);
    setIsUploading(true);

    try {
      await performUpload(overrideType);
    } catch (error) {
      setUploadError(`Upload failed: ${error.message}`);
      onProcessingComplete();
    } finally {
      setIsUploading(false);
      setLoadTypeVerification(null);
    }
  };

  const handleLoadTypeCancel = () => {
    setShowLoadTypeDialog(false);
    setLoadTypeVerification(null);
    setIsUploading(false);
    onProcessingComplete();
  };


  return (
    <Box>
      {/* File Selection */}
      <Box sx={{ mb: 3 }}>
        <Typography variant="subtitle1" gutterBottom>
          Select CSV File
        </Typography>
        
        <Input
          accept=".csv"
          style={{ display: 'none' }}
          id="file-upload"
          type="file"
          onChange={handleFileSelect}
          disabled={disabled}
        />
        <label htmlFor="file-upload">
          <Button
            variant="outlined"
            component="span"
            startIcon={<CloudUploadIcon />}
            disabled={disabled}
            sx={{ mr: 2 }}
          >
            Choose File
          </Button>
        </label>
        
        {selectedFile && (
          <Typography variant="body2" sx={{ mt: 1 }}>
            Selected: {selectedFile.name} ({(selectedFile.size / 1024 / 1024).toFixed(2)} MB)
          </Typography>
        )}
        
        {uploadError && (
          <Alert severity="error" sx={{ mt: 2 }}>
            {uploadError}
          </Alert>
        )}
      </Box>

      {/* Auto-Detection Section */}
      {selectedFile && (
        <Box sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>
            Format Detection
          </Typography>
          
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 2 }}>
            <Button
              variant={autoDetection.detected ? "outlined" : "contained"}
              color="secondary"
              onClick={detectFormat}
              disabled={disabled || autoDetection.isDetecting}
              startIcon={autoDetection.isDetecting ? <CircularProgress size={20} /> : null}
            >
              {autoDetection.isDetecting ? 'Detecting...' : 
               autoDetection.detected ? 'Re-Detect Format' : 'Auto-Detect Format'}
            </Button>
            
            {autoDetection.detected && (
              <Typography variant="body2" color="text.secondary">
                (Auto-detected on file selection)
              </Typography>
            )}
          </Box>

          {autoDetection.detected && autoDetection.suggestions && (
            <Alert 
              severity={autoDetection.confidence > 0.7 ? "success" : autoDetection.confidence > 0.4 ? "warning" : "info"} 
              sx={{ mb: 2 }}
            >
              <Box>
                <Typography variant="body2" sx={{ fontWeight: 'bold' }}>
                  Format Detection Results (Confidence: {(autoDetection.confidence * 100).toFixed(1)}%)
                </Typography>
                <Typography variant="body2">
                  • Column Delimiter: "{autoDetection.suggestions.column_delimiter}"
                  • Text Qualifier: "{autoDetection.suggestions.text_qualifier}"
                  • Has Header: {autoDetection.suggestions.has_header ? 'Yes' : 'No'}
                  • Has Trailer: {autoDetection.suggestions.has_trailer ? 'Yes' : 'No'}
                </Typography>
                {autoDetection.suggestions.trailer_line && (
                  <Typography variant="body2" sx={{ mt: 1, fontFamily: 'monospace', backgroundColor: '#f5f5f5', padding: '8px', borderRadius: '4px' }}>
                    <strong>Trailer Line:</strong> "{autoDetection.suggestions.trailer_line}"
                  </Typography>
                )}
                {autoDetection.analysis && (
                  <Typography variant="body2" sx={{ mt: 1 }}>
                    Detected {autoDetection.analysis.estimated_columns} columns, 
                    {autoDetection.analysis.sample_rows} sample rows, 
                    encoding: {autoDetection.analysis.encoding}
                  </Typography>
                )}
              </Box>
            </Alert>
          )}
        </Box>
      )}

      {/* CSV Format Configuration */}
      <Typography variant="subtitle1" gutterBottom>
        CSV Format Configuration
      </Typography>
      
      <Grid container spacing={2} sx={{ mb: 3 }}>
        {renderDelimiterSelect('header_delimiter', 'Header Delimiter')}
        {renderDelimiterSelect('column_delimiter', 'Column Delimiter')}
        {renderDelimiterSelect('row_delimiter', 'Row Delimiter')}
        {renderDelimiterSelect('text_qualifier', 'Text Qualifier')}
        
        <Grid item xs={12} md={6}>
          <TextField
            fullWidth
            type="number"
            label="Skip Lines After Header"
            value={formatConfig.skip_lines}
            onChange={(e) => handleFormatChange('skip_lines', parseInt(e.target.value) || 0)}
            inputProps={{ min: 0 }}
          />
        </Grid>
        
        <Grid item xs={12} md={6}>
          <TextField
            fullWidth
            label="Trailer Line (Optional)"
            value={formatConfig.trailer_line}
            onChange={(e) => handleFormatChange('trailer_line', e.target.value)}
            placeholder="e.g., TRAILER"
          />
        </Grid>
      </Grid>

      {/* Load Mode Selection */}
      <Typography variant="subtitle1" gutterBottom>
        Data Load Mode
      </Typography>
      
      <RadioGroup
        value={formatConfig.load_mode}
        onChange={(e) => handleFormatChange('load_mode', e.target.value)}
        sx={{ mb: 3 }}
      >
        <FormControlLabel 
          value="full" 
          control={<Radio />} 
          label="Full Load (Replace all existing data)" 
          disabled={disabled}
        />
        <FormControlLabel 
          value="append" 
          control={<Radio />} 
          label="Append Load (Add to existing data)" 
          disabled={disabled}
        />
      </RadioGroup>

      {/* Upload Button */}
      <Button
        variant="contained"
        onClick={handleUpload}
        disabled={!selectedFile || disabled || isUploading}
        startIcon={isUploading ? <CircularProgress size={20} /> : <CloudUploadIcon />}
        size="large"
      >
        {isUploading ? 'Processing...' : 'Upload and Process'}
      </Button>

      {isUploading && (
        <Box sx={{ mt: 2 }}>
          <LinearProgress />
          <Typography variant="body2" sx={{ mt: 1 }}>
            Uploading file and starting data ingestion...
          </Typography>
        </Box>
      )}

      {/* Load Type Warning Dialog */}
      <LoadTypeWarningDialog
        open={showLoadTypeDialog}
        onClose={handleLoadTypeCancel}
        onConfirm={handleLoadTypeOverride}
        verificationData={loadTypeVerification}
      />
    </Box>
  );
};

export default FileUploadComponent;