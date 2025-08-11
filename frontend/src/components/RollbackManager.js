import React, { useEffect, useState, useCallback } from 'react';
import { Box, Typography, Paper, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Button, Collapse, IconButton, Alert, Chip, Dialog, DialogTitle, DialogContent, DialogActions, TextField, MenuItem, Tooltip, Select, FormControl, InputLabel, Checkbox, ListItemText } from '@mui/material';
import { ExpandMore, ExpandLess, Restore } from '@mui/icons-material';

const RollbackManager = () => {
  const [backups, setBackups] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [expanded, setExpanded] = useState(new Set());
  const [selectedVersion, setSelectedVersion] = useState(null); // { base_name, version_id }
  const [versionRows, setVersionRows] = useState({}); // key: base_name|version -> { columns, rows, total_rows }
  const [rowLimit, setRowLimit] = useState(100); // default fetch size
  const [filters, setFilters] = useState({}); // legacy simple filters (cleared on change)
  const [valueFilters, setValueFilters] = useState({}); // column -> array of discrete selected values
  const [regexFilters, setRegexFilters] = useState({}); // column -> regex pattern
  const [confirm, setConfirm] = useState(null); // { base_name, version }
  const [actionMsg, setActionMsg] = useState('');

  const loadBackups = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const resp = await fetch('/backups');
      if (!resp.ok) throw new Error('Failed to load backups');
      const data = await resp.json();
      setBackups(data.backups || []);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadBackups(); }, [loadBackups]);

  const toggleExpand = (idx) => {
    const next = new Set(expanded);
    if (next.has(idx)) next.delete(idx); else next.add(idx);
    setExpanded(next);
  };

  const loadVersion = async (base_name, version_id, customLimit) => {
    const key = `${base_name}|${version_id}`;
    const limitToUse = customLimit || rowLimit;
    // Always refetch if limit changes
    if (versionRows[key] && versionRows[key].fetchedLimit === limitToUse) return; // cached with same limit
    try {
      const resp = await fetch(`/backups/${base_name}/versions/${version_id}?limit=${limitToUse}`);
      if (!resp.ok) throw new Error('Failed to load version rows');
      const data = await resp.json();
      setVersionRows(prev => ({ ...prev, [key]: { ...data, fetchedLimit: limitToUse } }));
    } catch (e) {
      setVersionRows(prev => ({ ...prev, [key]: { error: e.message, rows: [], columns: [] } }));
    }
  };

  const handleRollback = async () => {
    if (!confirm) return;
    const { base_name, version_id } = confirm;
    setActionMsg('');
    try {
      const resp = await fetch(`/backups/${base_name}/rollback/${version_id}`, { method: 'POST' });
      const data = await resp.json();
      if (!resp.ok) throw new Error(data.detail || 'Rollback failed');
      setActionMsg(`Rollback success: main_rows=${data.main_rows} stage_rows=${data.stage_rows}`);
      loadBackups();
    } catch (e) {
      setActionMsg(`Rollback failed: ${e.message}`);
    } finally {
      setConfirm(null);
    }
  };

  return (
    <Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
        <Typography variant="h6">Rollback Dataset Versions</Typography>
        <Button variant="outlined" size="small" onClick={loadBackups} disabled={loading}>{loading ? 'Loading...' : 'Refresh'}</Button>
      </Box>
      {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
      {actionMsg && <Alert severity="info" sx={{ mb: 2 }}>{actionMsg}</Alert>}
      <TableContainer component={Paper} variant="outlined">
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell width={40}></TableCell>
              <TableCell>Backup Table</TableCell>
              <TableCell>Main/Stage</TableCell>
              <TableCell>Versions</TableCell>
              <TableCell>Latest</TableCell>
              <TableCell>Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {backups.map((b, idx) => {
              const ok = b.has_main && b.has_stage;
              return (
                <React.Fragment key={b.backup_table}>
                  <TableRow hover>
                    <TableCell>
                      <IconButton size="small" onClick={() => toggleExpand(idx)}>
                        {expanded.has(idx) ? <ExpandLess /> : <ExpandMore />}
                      </IconButton>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>{b.backup_table}</Typography>
                      {!ok && (
                        <Typography variant="caption" color="error">Missing related table(s)</Typography>
                      )}
                    </TableCell>
                    <TableCell>
                      <Chip label={b.has_main ? 'Main OK' : 'Main MISSING'} size="small" color={b.has_main ? 'success' : 'error'} sx={{ mr: 0.5 }} />
                      <Chip label={b.has_stage ? 'Stage OK' : 'Stage MISSING'} size="small" color={b.has_stage ? 'success' : 'warning'} />
                    </TableCell>
                    <TableCell>{b.version_count}</TableCell>
                    <TableCell>{b.latest_version ?? '-'}</TableCell>
                    <TableCell>
                      <Box sx={{ display: 'flex', gap: 1 }}>
                        {b.latest_version && ok && (
                          <Button size="small" variant="outlined" onClick={() => { setSelectedVersion({ base_name: b.base_name, version_id: b.latest_version }); loadVersion(b.base_name, b.latest_version); setFilters({}); }}>View Latest</Button>
                        )}
                        {ok && (
                          <Button
                            size="small"
                            variant="outlined"
                            color="secondary"
                            onClick={async () => {
                              const url = `/backups/${b.base_name}/export-main`;
                              try {
                                const resp = await fetch(url, { headers: { 'Accept': 'text/csv' } });
                                if (!resp.ok) {
                                  alert('Export failed: ' + resp.status);
                                  return;
                                }
                                const blob = await resp.blob();
                                const dlUrl = window.URL.createObjectURL(blob);
                                const a = document.createElement('a');
                                a.href = dlUrl;
                                // Try to derive filename from Content-Disposition header
                                let filename = null;
                                const cd = resp.headers.get('Content-Disposition');
                                if (cd) {
                                  const match = cd.match(/filename\*=UTF-8''([^;]+)|filename="?([^";]+)"?/);
                                  if (match) {
                                    filename = decodeURIComponent(match[1] || match[2] || '').trim();
                                  }
                                }
                                if (!filename || filename === '') {
                                  // Fallback to new pattern: table.yyyyMMdd.csv (UTC date)
                                  const utcDate = new Date().toISOString().slice(0,10).replace(/-/g,'');
                                  filename = `${b.base_name}.${utcDate}.csv`;
                                }
                                a.download = filename;
                                document.body.appendChild(a);
                                a.click();
                                document.body.removeChild(a);
                                setTimeout(() => window.URL.revokeObjectURL(dlUrl), 5000);
                              } catch (e) {
                                // Fallback attempt
                                window.open(url, '_blank');
                              }
                            }}
                          >
                            Export CSV
                          </Button>
                        )}
                      </Box>
                    </TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={6}>
                      <Collapse in={expanded.has(idx)} timeout="auto" unmountOnExit>
                        <Box sx={{ p: 2, backgroundColor: '#fafafa' }}>
                          <Typography variant="subtitle2" gutterBottom>Versions</Typography>
                          {b.version_count === 0 && <Typography variant="body2" color="text.secondary">No versions</Typography>}
                          {b.version_count > 0 && (
                            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                              {Array.from({ length: b.version_count }, (_, i) => b.latest_version - i).map(v => (
                                <Chip key={v} label={`v${v}`} variant={selectedVersion?.version_id === v && selectedVersion?.base_name === b.base_name ? 'filled' : 'outlined'} clickable onClick={() => { setSelectedVersion({ base_name: b.base_name, version_id: v }); loadVersion(b.base_name, v); setFilters({}); }} />
                              ))}
                            </Box>
                          )}
                        </Box>
                      </Collapse>
                    </TableCell>
                  </TableRow>
                </React.Fragment>
              );
            })}
          </TableBody>
        </Table>
      </TableContainer>

      {/* Version Detail Dialog */}
      <Dialog open={!!selectedVersion} onClose={() => setSelectedVersion(null)} maxWidth="xl" fullWidth>
        <DialogTitle>
          Version Details - {selectedVersion?.base_name} v{selectedVersion?.version_id}
        </DialogTitle>
        <DialogContent dividers>
          {selectedVersion && (() => {
            const key = `${selectedVersion.base_name}|${selectedVersion.version_id}`;
            const data = versionRows[key];
            if (!data) return <Typography variant="body2">Loading...</Typography>;
            if (data.error) return <Alert severity="error">{data.error}</Alert>;
            const filteredRows = data.rows.filter(r => {
              return data.columns.every((col, cIdx) => {
                const cell = r[cIdx];
                const cellStr = (cell === null || cell === undefined) ? '' : String(cell);
                const pattern = regexFilters[col];
                if (pattern && pattern.trim() !== '') {
                  try {
                    const re = new RegExp(pattern, 'i');
                    if (!re.test(cellStr)) return false;
                  } catch (e) {
                    // invalid regex -> ignore filter
                  }
                } else {
                  const valSel = valueFilters[col];
                  if (valSel && valSel.length > 0) {
                    if (!valSel.includes(cellStr)) return false;
                  }
                }
                return true;
              });
            });
            return (
              <Box>
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 2, mb: 1, alignItems: 'center' }}>
                  <Typography variant="caption">Total rows in version: {data.total_rows}</Typography>
                  <Typography variant="caption">Showing {filteredRows.length} / {data.rows.length} fetched (limit {data.fetchedLimit})</Typography>
                  <Button
                    size="small"
                    variant="outlined"
                    onClick={async () => {
                      try {
                        const url = `/backups/${selectedVersion.base_name}/versions/${selectedVersion.version_id}/export`;
                        const resp = await fetch(url, { headers: { 'Accept': 'text/csv' } });
                        if (!resp.ok) {
                          alert('Export failed: ' + resp.status);
                          return;
                        }
                        const blob = await resp.blob();
                        const dlUrl = window.URL.createObjectURL(blob);
                        const a = document.createElement('a');
                        a.href = dlUrl;
                        // Parse Content-Disposition filename or fallback
                        let filename = null;
                        const cd = resp.headers.get('Content-Disposition');
                        if (cd) {
                          const match = cd.match(/filename\*=UTF-8''([^;]+)|filename="?([^";]+)"?/);
                          if (match) filename = decodeURIComponent(match[1] || match[2] || '').trim();
                        }
                        if (!filename) {
                          const utcDate = new Date().toISOString().slice(0,10).replace(/-/g,'');
                          filename = `${selectedVersion.base_name}.${utcDate}.csv`;
                        }
                        a.download = filename;
                        document.body.appendChild(a);
                        a.click();
                        document.body.removeChild(a);
                        setTimeout(() => window.URL.revokeObjectURL(dlUrl), 5000);
                      } catch (e) {
                        console.error('Version export failed', e);
                      }
                    }}
                  >Export This Version</Button>
                  <TextField
                    select
                    label="Row Limit"
                    size="small"
                    value={rowLimit}
                    onChange={(e) => {
                      const newLimit = parseInt(e.target.value, 10);
                      setRowLimit(newLimit);
                      loadVersion(selectedVersion.base_name, selectedVersion.version_id, newLimit);
                    }}
                    sx={{ width: 140 }}
                  >
                    {[50,100,200,500,750,1000].map(v => <MenuItem key={v} value={v}>{v}</MenuItem>)}
                  </TextField>
                  <Button size="small" onClick={() => { loadVersion(selectedVersion.base_name, selectedVersion.version_id, rowLimit); }}>Reload</Button>
                  <Button size="small" onClick={() => { setFilters({}); setRegexFilters({}); setValueFilters({}); }}>Clear Filters</Button>
                </Box>
                <TableContainer component={Paper} variant="outlined" sx={{ maxHeight: 500 }}>
                  <Table size="small" stickyHeader>
                    <TableHead>
                      <TableRow>
                        {data.columns.map((c, idx) => {
                          // Collect up to 20 distinct values from fetched rows
                          const seen = new Set();
                          const distinct = [];
                          for (let r of data.rows) {
                            const val = r[idx];
                            const valStr = (val === null || val === undefined) ? '' : String(val);
                            if (!seen.has(valStr)) {
                              seen.add(valStr);
                              distinct.push(valStr);
                              if (distinct.length >= 20) break;
                            }
                          }
                          distinct.sort((a,b)=> a.localeCompare(b));
                          const selectedVals = valueFilters[c] || [];
                          const regexVal = regexFilters[c] || '';
                          return (
                            <TableCell key={c}>
                              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5, minWidth: 140 }}>
                                <strong style={{ fontSize: '0.75rem' }}>{c}</strong>
                                <FormControl size="small" sx={{ width: '100%' }}>
                                  <Select
                                    multiple
                                    value={regexVal ? [] : selectedVals}
                                    displayEmpty
                                    onChange={(e) => {
                                      // Selecting values clears regex
                                      setRegexFilters(prev => ({ ...prev, [c]: '' }));
                                      const val = e.target.value;
                                      setValueFilters(prev => ({ ...prev, [c]: typeof val === 'string' ? val.split(',') : val }));
                                    }}
                                    renderValue={(sel) => {
                                      if (regexVal) return `RegEx: ${regexVal}`;
                                      if (!sel || sel.length === 0) return 'All';
                                      return sel.slice(0,3).join(', ') + (sel.length>3 ? '…':'');
                                    }}
                                    MenuProps={{ PaperProps: { style: { maxHeight: 340, paddingTop: 4 } } }}
                                    sx={{ fontSize: '0.65rem' }}
                                  >
                                    {/* Regex input inside menu */}
                                    <MenuItem disableRipple disableGutters divider sx={{ py: 0.5, px: 1, alignItems: 'flex-start' }}>
                                      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5, width: '100%' }}>
                                        <TextField
                                          size="small"
                                          placeholder="regex (optional)"
                                          value={regexVal}
                                          onChange={(e) => {
                                            const v = e.target.value;
                                            setRegexFilters(prev => ({ ...prev, [c]: v }));
                                            if (v) {
                                              // Clear value selections when regex active
                                              setValueFilters(prev => ({ ...prev, [c]: [] }));
                                            }
                                          }}
                                          InputProps={{ sx: { fontSize: '0.65rem' } }}
                                          onClick={e => e.stopPropagation()}
                                        />
                                        {regexVal && (
                                          <Button size="small" variant="text" sx={{ alignSelf: 'flex-end', fontSize: '0.55rem', mt: -0.5 }} onClick={(e) => { e.stopPropagation(); setRegexFilters(prev => ({ ...prev, [c]: '' })); }}>Clear RegEx</Button>
                                        )}
                                      </Box>
                                    </MenuItem>
                                    {distinct.map(v => {
                                      const label = v === '' ? '(blank)' : v;
                                      const selected = selectedVals.indexOf(v) > -1;
                                      return (
                                        <MenuItem key={v === '' ? '__blank__' : v} value={v} dense>
                                          <Checkbox size="small" checked={selected} />
                                          <ListItemText primaryTypographyProps={{ fontSize: '0.6rem' }} primary={label} />
                                        </MenuItem>
                                      );
                                    })}
                                  </Select>
                                </FormControl>
                              </Box>
                            </TableCell>
                          );
                        })}
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {filteredRows.map((r, i) => (
                        <TableRow key={i}>
                          {r.map((val, j) => <TableCell key={j}>{val === null ? '' : String(val)}</TableCell>)}
                        </TableRow>
                      ))}
                      {filteredRows.length === 0 && (
                        <TableRow>
                          <TableCell colSpan={data.columns.length}>
                            <Typography variant="caption" color="text.secondary">No rows match filters.</Typography>
                          </TableCell>
                        </TableRow>
                      )}
                    </TableBody>
                  </Table>
                </TableContainer>
              </Box>
            );
          })()}
        </DialogContent>
        <DialogActions>
          {selectedVersion && (
            <Button color="error" startIcon={<Restore />} onClick={() => setConfirm(selectedVersion)}>Rollback to v{selectedVersion.version_id}</Button>
          )}
          <Button onClick={() => setSelectedVersion(null)}>Close</Button>
        </DialogActions>
      </Dialog>

      {/* Confirm Dialog */}
      <Dialog open={!!confirm} onClose={() => setConfirm(null)}>
        <DialogTitle>Confirm Rollback</DialogTitle>
        <DialogContent>
          <Typography variant="body2">Are you sure you want to rollback <strong>{confirm?.base_name}</strong> to version <strong>{confirm?.version_id}</strong>? This will replace current data.</Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setConfirm(null)}>Cancel</Button>
          <Button color="error" variant="contained" onClick={handleRollback}>Confirm</Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default RollbackManager;
