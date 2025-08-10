import React, { useState, useEffect } from 'react';
import {
  Container,
  Typography,
  Paper,
  Box,
  Alert,
  CircularProgress
} from '@mui/material';
import FileUploadComponent from './components/FileUploadComponent';
import ProgressDisplay from './components/ProgressDisplay';
import LogsDisplay from './components/LogsDisplay';
import ConfigurationPanel from './components/ConfigurationPanel';

function App() {
  const [systemStatus, setSystemStatus] = useState('loading');
  const [config, setConfig] = useState(null);
  const [uploadProgress, setUploadProgress] = useState(null); // percent for file upload only
  const [ingestionData, setIngestionData] = useState(null);    // object from /progress/{key}
  const [progressKey, setProgressKey] = useState(null);
  const [isProcessing, setIsProcessing] = useState(false);     // disable inputs while ingesting

  useEffect(() => {
    // Check system status and load configuration on startup
    checkSystemHealth();
    loadConfiguration();
  }, []);

  const checkSystemHealth = async () => {
    try {
      const response = await fetch('/');
      if (response.ok) {
        setSystemStatus('healthy');
      } else {
        setSystemStatus('error');
      }
    } catch (error) {
      setSystemStatus('error');
    }
  };

  const loadConfiguration = async () => {
    try {
      const response = await fetch('/config');
      if (response.ok) {
        const configData = await response.json();
        setConfig(configData);
      }
    } catch (error) {
      console.error('Failed to load configuration:', error);
    }
  };

  const handleUploadStart = () => {
    setIsProcessing(true);
    setUploadProgress(0);
    setIngestionData(null);
    setProgressKey(null);
  };

  const handleUploadComplete = (result) => {
    setUploadProgress(100); // upload itself done
    if (result.progress_key) {
      setProgressKey(result.progress_key);
      // initialize optimistic state while first poll arrives
      setIngestionData({
        found: true,
        stage: 'starting',
        percent: 0,
        inserted: 0,
        total: null,
        done: false,
        error: null
      });
    }
  };

  // Poll backend for ingestion progress while progressKey present and not done
  useEffect(() => {
    if (!progressKey) return;
    let cancelled = false;
    let intervalId = null;
    
    const poll = async () => {
      if (cancelled) return;
      try {
        const resp = await fetch(`/progress/${progressKey}`);
        if (!resp.ok) return;
        const data = await resp.json();
        if (cancelled) return;
        if (data.found) {
          setIngestionData(data);
          if (data.done || data.error) {
            console.log('Polling stopped - job completed:', data.done, 'error:', data.error);
            setIsProcessing(false);
            cancelled = true;
            if (intervalId) {
              clearInterval(intervalId);
              intervalId = null;
            }
            return; // stop polling
          }
        }
      } catch (e) {
        console.log('Poll error:', e);
        // swallow errors; next tick may succeed
      }
    };
    
    // immediate poll then interval (2 seconds for reduced server load)
    poll();
    intervalId = setInterval(poll, 2000);
    
    return () => {
      cancelled = true;
      if (intervalId) {
        clearInterval(intervalId);
        intervalId = null;
      }
    };
  }, [progressKey]);

  const handleProcessingComplete = () => {
    // retained for backward compatibility; explicit stop
    setIsProcessing(false);
  };

  return (
    <Container maxWidth="lg" sx={{ py: 4 }}>
      {/* Header */}
      <Paper elevation={3} sx={{ p: 3, mb: 3 }}>
        <Typography variant="h4" component="h1" gutterBottom>
          Reference Data Auto Ingest System
        </Typography>
        <Typography variant="subtitle1" color="text.secondary">
          Automated CSV data ingestion to SQL Server
        </Typography>
        
        {systemStatus === 'loading' && (
          <Box sx={{ display: 'flex', alignItems: 'center', mt: 2 }}>
            <CircularProgress size={20} sx={{ mr: 1 }} />
            <Typography variant="body2">Checking system status...</Typography>
          </Box>
        )}
        {systemStatus === 'healthy' && (
          <Alert severity="success" sx={{ mt: 2, mb: 2 }}>
            System is healthy and ready to process files
          </Alert>
        )}
        {systemStatus === 'error' && (
          <Alert severity="error" sx={{ mt: 2 }}>
            System is not responding. Please check the backend service.
          </Alert>
        )}

        {/* Inlined System Configuration (summary + expandable details) */}
        {systemStatus === 'healthy' && config && (
          <Box sx={{ mt: 2 }}>
            <ConfigurationPanel config={config} />
          </Box>
        )}
      </Paper>

      {/* Main Content */}
      {systemStatus === 'healthy' && config && (
        <>
          {/* File Upload Section */}
          <Paper elevation={3} sx={{ p: 3, mb: 3 }}>
            <Typography variant="h6" gutterBottom>
              File Upload & Configuration
            </Typography>
            
            <FileUploadComponent
              config={config}
              onUploadStart={handleUploadStart}
              onUploadComplete={handleUploadComplete}
              onIngestionProgress={() => { /* deprecated callback retained */ }}
              onProcessingComplete={handleProcessingComplete}
              disabled={isProcessing}
            />
          </Paper>

          {/* Progress Display */}
      {(uploadProgress !== null || ingestionData) && (
            <Paper elevation={3} sx={{ p: 3, mb: 3 }}>
              <Typography variant="h6" gutterBottom>
                Processing Status
              </Typography>
              
              <ProgressDisplay
                uploadProgress={uploadProgress}
                ingestionData={ingestionData}
                progressKey={progressKey}
                onCancel={() => {
                  if (!progressKey) {
                    console.log('Cancel clicked but no progressKey');
                    return;
                  }
                  console.log('Sending IMMEDIATE cancel request for progressKey:', progressKey, 'at', new Date().toISOString());
                  
                  // Immediately update local state to show cancellation is in progress
                  setIngestionData(prev => ({
                    ...prev,
                    canceled: true,
                    stage: 'canceling',
                    error: 'Cancellation requested'
                  }));

                  // Use XMLHttpRequest to bypass browser fetch queuing
                  const xhr = new XMLHttpRequest();
                  xhr.open('POST', `/progress/${progressKey}/cancel`, true);
                  xhr.onload = function() {
                    console.log('Cancel XHR response status:', xhr.status, 'at', new Date().toISOString());
                    console.log('Cancel XHR response:', xhr.responseText);
                  };
                  xhr.onerror = function() {
                    console.log('Cancel XHR request failed at', new Date().toISOString());
                  };
                  xhr.send();
                  
                  // DIRECT backend connection bypassing frontend proxy
                  const directXhr = new XMLHttpRequest();
                  directXhr.open('POST', `http://localhost:8000/progress/${progressKey}/cancel`, true);
                  directXhr.onload = function() {
                    console.log('DIRECT Cancel XHR response status:', directXhr.status, 'at', new Date().toISOString());
                    console.log('DIRECT Cancel XHR response:', directXhr.responseText);
                  };
                  directXhr.onerror = function() {
                    console.log('DIRECT Cancel XHR request failed at', new Date().toISOString());
                  };
                  directXhr.send();
                  
                  // Also try a backup fetch with different URL to force new connection
                  setTimeout(() => {
                    fetch(`/progress/${progressKey}/cancel?t=${Date.now()}`, { 
                      method: 'POST',
                      cache: 'no-cache',
                      keepalive: false
                    }).then(response => {
                      console.log('Backup cancel response:', response.status, 'at', new Date().toISOString());
                    }).catch(e => {
                      console.log('Backup cancel failed:', e);
                    });
                  }, 10);
                }}
                onReset={() => {
                  // Reset all progress states
                  setUploadProgress(null);
                  setIngestionData(null);
                  setProgressKey(null);
                  setIsProcessing(false);
                }}
              />
            </Paper>
          )}

          {/* Logs Display */}
          <Paper elevation={3} sx={{ p: 3 }}>
            <LogsDisplay />
          </Paper>
        </>
      )}
    </Container>
  );
}

export default App;