import React, { useState } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Typography,
  Alert,
  FormControl,
  FormLabel,
  RadioGroup,
  FormControlLabel,
  Radio,
  Box,
  Divider
} from '@mui/material';
import WarningIcon from '@mui/icons-material/Warning';

function LoadTypeWarningDialog({ 
  open, 
  onClose, 
  onConfirm, 
  verificationData 
}) {
  const [overrideType, setOverrideType] = useState('');

  const handleConfirm = () => {
    if (overrideType) {
      onConfirm(overrideType);
    }
  };

  const handleClose = () => {
    setOverrideType('');
    onClose();
  };

  if (!verificationData) return null;

  const {
    table_name,
    requested_load_mode,
    requested_load_type,
    determined_load_type,
    existing_load_types,
    explanation
  } = verificationData;

  return (
    <Dialog 
      open={open} 
      onClose={handleClose}
      maxWidth="md"
      fullWidth
      PaperProps={{
        sx: { borderRadius: 3 }
      }}
    >
      <DialogTitle sx={{ 
        display: 'flex', 
        alignItems: 'center', 
        gap: 2,
        backgroundColor: '#fff3cd',
        color: '#856404',
        borderBottom: '1px solid #ffeaa7'
      }}>
        <WarningIcon sx={{ color: '#f39c12' }} />
        <Typography variant="h6" sx={{ fontWeight: 600 }}>
          Load Type Mismatch Detected
        </Typography>
      </DialogTitle>
      
      <DialogContent sx={{ p: 3 }}>
        <Alert severity="warning" sx={{ mb: 3, borderRadius: 2 }}>
          <Typography variant="body1" sx={{ fontWeight: 500, mb: 1 }}>
            The system has detected a load type mismatch for table "{table_name}".
          </Typography>
          <Typography variant="body2">
            {explanation}
          </Typography>
        </Alert>

        <Box sx={{ mb: 3 }}>
          <Typography variant="h6" gutterBottom sx={{ color: 'text.primary', fontWeight: 600 }}>
            Current Situation:
          </Typography>
          
          <Box sx={{ pl: 2, py: 1, backgroundColor: '#f8f9fa', borderRadius: 2, mb: 2 }}>
            <Typography variant="body2" sx={{ mb: 1 }}>
              <strong>Requested Mode:</strong> {requested_load_mode} load (would use load type '{requested_load_type}')
            </Typography>
            <Typography variant="body2" sx={{ mb: 1 }}>
              <strong>System Decision:</strong> Based on existing data, will use load type '{determined_load_type}'
            </Typography>
            <Typography variant="body2">
              <strong>Existing Load Types:</strong> {existing_load_types.length > 0 ? existing_load_types.join(', ') : 'None'}
            </Typography>
          </Box>
        </Box>

        <Divider sx={{ my: 2 }} />

        <Typography variant="h6" gutterBottom sx={{ color: 'text.primary', fontWeight: 600 }}>
          Override Load Type:
        </Typography>
        
        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          You can override the load type for this upload only. This will not change the existing data or future load type determination.
        </Typography>

        <FormControl component="fieldset" fullWidth>
          <FormLabel component="legend" sx={{ mb: 2 }}>
            Select the load type to enforce for this upload:
          </FormLabel>
          <RadioGroup
            value={overrideType}
            onChange={(e) => setOverrideType(e.target.value)}
          >
            <FormControlLabel 
              value="Full" 
              control={<Radio sx={{ color: 'primary.main' }} />} 
              label={
                <Box>
                  <Typography variant="body1" sx={{ fontWeight: 500 }}>
                    Full Load
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Force all new rows to use load type 'F' regardless of existing data patterns
                  </Typography>
                </Box>
              }
              sx={{ 
                mb: 1, 
                p: 2, 
                border: overrideType === 'Full' ? '2px solid #54B848' : '1px solid #e0e0e0',
                borderRadius: 2,
                backgroundColor: overrideType === 'Full' ? '#f0fff0' : 'transparent'
              }}
            />
            <FormControlLabel 
              value="Append" 
              control={<Radio sx={{ color: 'primary.main' }} />} 
              label={
                <Box>
                  <Typography variant="body1" sx={{ fontWeight: 500 }}>
                    Append Load
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Force all new rows to use load type 'A' regardless of existing data patterns
                  </Typography>
                </Box>
              }
              sx={{ 
                p: 2, 
                border: overrideType === 'Append' ? '2px solid #54B848' : '1px solid #e0e0e0',
                borderRadius: 2,
                backgroundColor: overrideType === 'Append' ? '#f0fff0' : 'transparent'
              }}
            />
          </RadioGroup>
        </FormControl>

        <Alert severity="info" sx={{ mt: 3, borderRadius: 2 }}>
          <Typography variant="body2">
            <strong>Note:</strong> This override only affects the current upload. Future uploads will still follow the standard load type determination rules based on existing data.
          </Typography>
        </Alert>
      </DialogContent>
      
      <DialogActions sx={{ p: 3, pt: 0 }}>
        <Button 
          onClick={handleClose} 
          variant="outlined"
          sx={{ mr: 1 }}
        >
          Cancel Upload
        </Button>
        <Button 
          onClick={handleConfirm} 
          variant="contained"
          disabled={!overrideType}
          sx={{ 
            backgroundColor: 'primary.main',
            '&:hover': { backgroundColor: 'primary.dark' }
          }}
        >
          Proceed with {overrideType} Load Type
        </Button>
      </DialogActions>
    </Dialog>
  );
}

export default LoadTypeWarningDialog;