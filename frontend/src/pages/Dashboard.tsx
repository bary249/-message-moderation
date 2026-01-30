import React, { useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Box,
  Container,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Chip,
  IconButton,
  AppBar,
  Toolbar,
  Button,
  LinearProgress,
  Pagination,
  Slider,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  SelectChangeEvent,
  Tabs,
  Tab,
  Checkbox,
  TextField,
  Alert,
} from '@mui/material';
import {
  Visibility as ViewIcon,
  Logout as LogoutIcon,
  CloudDownload as CloudDownloadIcon,
  CheckCircle as CheckCircleIcon,
  Psychology as ScoreIcon,
} from '@mui/icons-material';
import { getQueue, ingestMessages, reviewMessage, fetchMessagesByDate, scoreBatch, Message } from '../services/api';
import { useAuth } from '../services/AuthContext';
import { format } from 'date-fns';

const Dashboard: React.FC = () => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [loading, setLoading] = useState(true);
  const [ingesting, setIngesting] = useState(false);
  const [page, setPage] = useState(1);
  const [totalCount, setTotalCount] = useState(0);
  const [scoreRange, setScoreRange] = useState<number[]>([0, 100]);
  const [sortBy, setSortBy] = useState<string>('time');
  const [activeTab, setActiveTab] = useState<'pending' | 'reviewed'>('pending');
  const [selectedIds, setSelectedIds] = useState<number[]>([]);
  const [selectedDate, setSelectedDate] = useState<string>('');
  const [fetching, setFetching] = useState(false);
  const [scoring, setScoring] = useState(false);
  const [unscoredCount, setUnscoredCount] = useState<number | null>(null);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const { user, logout } = useAuth();
  const navigate = useNavigate();

  const fetchMessages = useCallback(async () => {
    setLoading(true);
    try {
      const data = await getQueue(page, activeTab);
      
      // Filter by score range
      let filtered = data.pending_messages.filter(
        (m) => m.moderation_score * 100 >= scoreRange[0] && m.moderation_score * 100 <= scoreRange[1]
      );
      
      // Sort
      if (sortBy === 'score') {
        filtered = filtered.sort((a, b) => b.moderation_score - a.moderation_score);
      } else if (sortBy === 'group') {
        filtered = filtered.sort((a, b) => 
          (a.group_name || a.group_id || '').localeCompare(b.group_name || b.group_id || '')
        );
      } else if (sortBy === 'time_asc') {
        filtered = filtered.sort((a, b) => 
          new Date(a.message_timestamp || a.created_at).getTime() - 
          new Date(b.message_timestamp || b.created_at).getTime()
        );
      } else {
        // Default: time descending (newest first)
        filtered = filtered.sort((a, b) => 
          new Date(b.message_timestamp || b.created_at).getTime() - 
          new Date(a.message_timestamp || a.created_at).getTime()
        );
      }
      
      setMessages(filtered);
      setTotalCount(data.total_count);
    } catch (error) {
      console.error('Failed to fetch messages:', error);
    }
    setLoading(false);
  }, [page, scoreRange, sortBy, activeTab]);

  useEffect(() => {
    fetchMessages();
  }, [page, scoreRange, sortBy, activeTab]);

  const handleScoreChange = (_: Event, newValue: number | number[]) => {
    setScoreRange(newValue as number[]);
  };

  const handleSortChange = (event: SelectChangeEvent) => {
    setSortBy(event.target.value);
  };

  const handlePullMessages = async () => {
    setIngesting(true);
    try {
      await ingestMessages();
      // Poll for messages until they appear (max 45 seconds)
      let attempts = 0;
      const maxAttempts = 9; // 9 * 5s = 45 seconds
      const pollInterval = setInterval(async () => {
        attempts++;
        try {
          const data = await getQueue(1, 'pending');
          if (data.pending_messages.length > 0 || attempts >= maxAttempts) {
            clearInterval(pollInterval);
            setIngesting(false);
            fetchMessages();
          }
        } catch (e) {
          console.error('Poll error:', e);
          clearInterval(pollInterval);
          setIngesting(false);
        }
      }, 5000);
    } catch (error) {
      console.error('Failed to start ingestion:', error);
      setIngesting(false);
    }
  };

  const handleFetchByDate = async () => {
    if (!selectedDate) return;
    setFetching(true);
    setStatusMessage(null);
    try {
      const result = await fetchMessagesByDate(selectedDate);
      setStatusMessage(`Fetched ${result.fetched_from_snowflake} messages, ${result.new_messages_saved} new (unscored)`);
      setUnscoredCount(result.new_messages_saved);
      fetchMessages();
    } catch (error) {
      console.error('Failed to fetch by date:', error);
      setStatusMessage('Error fetching messages');
    }
    setFetching(false);
  };

  const handleScoreBatch = async () => {
    setScoring(true);
    setStatusMessage(null);
    try {
      const result = await scoreBatch(20);
      if (result.status === 'complete') {
        setStatusMessage('All messages scored!');
        setUnscoredCount(0);
      } else {
        setStatusMessage(`Scored ${result.scored} messages in ${result.elapsed_seconds}s. ${result.remaining} remaining.`);
        setUnscoredCount(result.remaining);
      }
      fetchMessages();
    } catch (error) {
      console.error('Failed to score batch:', error);
      setStatusMessage('Error scoring messages');
    }
    setScoring(false);
  };

  const handleTabChange = (_: React.SyntheticEvent, newValue: 'pending' | 'reviewed') => {
    setActiveTab(newValue);
    setPage(1);
    setSelectedIds([]);
  };

  const handleSelectAll = (checked: boolean) => {
    if (checked) {
      setSelectedIds(messages.map(m => m.id));
    } else {
      setSelectedIds([]);
    }
  };

  const handleSelectOne = (id: number, checked: boolean) => {
    if (checked) {
      setSelectedIds(prev => [...prev, id]);
    } else {
      setSelectedIds(prev => prev.filter(i => i !== id));
    }
  };

  const handleMarkReviewed = async (id: number) => {
    try {
      await reviewMessage(id, 'reviewed');
      setMessages(prev => prev.filter(m => m.id !== id));
      setSelectedIds(prev => prev.filter(i => i !== id));
    } catch (error) {
      console.error('Failed to mark as reviewed:', error);
    }
  };

  const handleBulkMarkReviewed = async () => {
    try {
      await Promise.all(selectedIds.map(id => reviewMessage(id, 'reviewed')));
      setMessages(prev => prev.filter(m => !selectedIds.includes(m.id)));
      setSelectedIds([]);
    } catch (error) {
      console.error('Failed to bulk mark as reviewed:', error);
    }
  };

  const getScoreColor = (score: number): 'success' | 'warning' | 'error' => {
    if (score < 0.3) return 'success';
    if (score < 0.6) return 'warning';
    return 'error';
  };


  return (
    <Box sx={{ flexGrow: 1 }}>
      <AppBar position="static">
        <Toolbar>
          <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
            Message Moderation Dashboard
          </Typography>
          <Typography variant="body2" sx={{ mr: 2 }}>
            {user}
          </Typography>
          <Button color="inherit" onClick={logout} startIcon={<LogoutIcon />}>
            Logout
          </Button>
        </Toolbar>
      </AppBar>

      {/* Loading overlay */}
      {ingesting && (
        <Box
          sx={{
            position: 'fixed',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            bgcolor: 'rgba(0, 0, 0, 0.6)',
            zIndex: 9999,
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
          }}
        >
          <Typography variant="h5" color="white" sx={{ mb: 2 }}>
            Loading & Scoring Messages...
          </Typography>
          <LinearProgress sx={{ width: 300 }} />
          <Typography variant="body2" color="grey.400" sx={{ mt: 2 }}>
            This may take up to 30 seconds
          </Typography>
        </Box>
      )}

      <Container maxWidth="xl" sx={{ mt: 3 }}>
        {/* Tabs */}
        <Paper sx={{ mb: 2 }}>
          <Tabs value={activeTab} onChange={handleTabChange}>
            <Tab label="Pending Review" value="pending" />
            <Tab label="Reviewed" value="reviewed" />
          </Tabs>
        </Paper>

        {/* Date Picker & Fetch Controls */}
        <Paper sx={{ p: 2, mb: 2, display: 'flex', alignItems: 'center', gap: 2, flexWrap: 'wrap' }}>
          <TextField
            type="date"
            size="small"
            label="Select Date"
            value={selectedDate}
            onChange={(e) => setSelectedDate(e.target.value)}
            InputLabelProps={{ shrink: true }}
            sx={{ width: 180 }}
          />
          <Button
            variant="contained"
            onClick={handleFetchByDate}
            disabled={!selectedDate || fetching}
            startIcon={<CloudDownloadIcon />}
          >
            {fetching ? 'Fetching...' : 'Fetch Messages'}
          </Button>
          <Button
            variant="contained"
            color="secondary"
            onClick={handleScoreBatch}
            disabled={scoring || unscoredCount === 0}
            startIcon={<ScoreIcon />}
          >
            {scoring ? 'Scoring...' : 'Score Next 20'}
          </Button>
          {unscoredCount !== null && unscoredCount > 0 && (
            <Chip label={`${unscoredCount} unscored`} color="warning" />
          )}
          {statusMessage && (
            <Alert severity="info" sx={{ py: 0 }}>{statusMessage}</Alert>
          )}
        </Paper>

        {/* Filter Controls */}
        <Paper sx={{ p: 2, mb: 2, display: 'flex', alignItems: 'center', gap: 4 }}>
          <Box sx={{ width: 300 }}>
            <Typography variant="body2" gutterBottom>Score Range: {scoreRange[0]}% - {scoreRange[1]}%</Typography>
            <Slider
              value={scoreRange}
              onChange={handleScoreChange}
              valueLabelDisplay="auto"
              min={0}
              max={100}
            />
          </Box>
          <FormControl size="small" sx={{ minWidth: 180 }}>
            <InputLabel>Sort By</InputLabel>
            <Select value={sortBy} label="Sort By" onChange={handleSortChange}>
              <MenuItem value="time">Newest First</MenuItem>
              <MenuItem value="time_asc">Oldest First</MenuItem>
              <MenuItem value="group">Group Name</MenuItem>
              <MenuItem value="score">Highest Score</MenuItem>
            </Select>
          </FormControl>
          <Typography variant="body2" color="text.secondary">
            {messages.length} messages shown
          </Typography>
          {selectedIds.length > 0 && activeTab === 'pending' && (
            <Button
              variant="contained"
              color="success"
              onClick={handleBulkMarkReviewed}
              startIcon={<CheckCircleIcon />}
            >
              Mark {selectedIds.length} as Reviewed
            </Button>
          )}
        </Paper>

        {loading && <LinearProgress />}

        <TableContainer component={Paper}>
          <Table size="small">
            <TableHead>
              <TableRow>
                {activeTab === 'pending' && (
                  <TableCell padding="checkbox">
                    <Checkbox
                      indeterminate={selectedIds.length > 0 && selectedIds.length < messages.length}
                      checked={messages.length > 0 && selectedIds.length === messages.length}
                      onChange={(e) => handleSelectAll(e.target.checked)}
                    />
                  </TableCell>
                )}
                <TableCell>ID</TableCell>
                <TableCell>Message Preview</TableCell>
                <TableCell>Client / Building / Group</TableCell>
                <TableCell>Timestamp</TableCell>
                <TableCell>Score</TableCell>
                <TableCell>Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {messages.map((msg) => (
                <TableRow key={msg.id} hover selected={selectedIds.includes(msg.id)}>
                  {activeTab === 'pending' && (
                    <TableCell padding="checkbox">
                      <Checkbox
                        checked={selectedIds.includes(msg.id)}
                        onChange={(e) => handleSelectOne(msg.id, e.target.checked)}
                      />
                    </TableCell>
                  )}
                  <TableCell>{msg.id}</TableCell>
                  <TableCell sx={{ maxWidth: 400 }}>
                    <Typography variant="body2" noWrap>
                      {msg.processed_message?.substring(0, 120) || msg.original_message?.substring(0, 120)}...
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2" noWrap sx={{ fontWeight: 500 }}>
                      {msg.client_name || 'N/A'}
                    </Typography>
                    <Typography variant="caption" color="text.secondary" noWrap>
                      {msg.building_name || msg.building_id?.substring(0, 8)} / {msg.group_name || msg.group_id?.substring(0, 8)}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    {msg.message_timestamp 
                      ? format(new Date(msg.message_timestamp), 'MMM d HH:mm')
                      : format(new Date(msg.created_at), 'MMM d HH:mm')
                    }
                  </TableCell>
                  <TableCell>
                    <Chip
                      label={`${(msg.moderation_score * 100).toFixed(0)}%`}
                      color={getScoreColor(msg.moderation_score)}
                      size="small"
                    />
                  </TableCell>
                  <TableCell>
                    <IconButton
                      size="small"
                      onClick={() => navigate(`/message/${msg.id}`)}
                      title="View Details"
                    >
                      <ViewIcon />
                    </IconButton>
                    {activeTab === 'pending' && (
                      <IconButton
                        size="small"
                        color="success"
                        onClick={() => handleMarkReviewed(msg.id)}
                        title="Mark as Reviewed"
                      >
                        <CheckCircleIcon />
                      </IconButton>
                    )}
                  </TableCell>
                </TableRow>
              ))}
              {messages.length === 0 && !loading && activeTab === 'pending' && (
                <TableRow>
                  <TableCell colSpan={7} align="center">
                    <Box sx={{ py: 8, display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                      <CloudDownloadIcon sx={{ fontSize: 64, color: 'grey.400', mb: 2 }} />
                      <Typography variant="h6" color="text.secondary">
                        No messages to review
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        Select a date above and click "Fetch Messages"
                      </Typography>
                    </Box>
                  </TableCell>
                </TableRow>
              )}
              {messages.length === 0 && !loading && activeTab === 'reviewed' && (
                <TableRow>
                  <TableCell colSpan={7} align="center">
                    <Typography color="text.secondary" sx={{ py: 4 }}>
                      No reviewed messages yet
                    </Typography>
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </TableContainer>

        <Box sx={{ display: 'flex', justifyContent: 'center', mt: 2 }}>
          <Pagination
            count={Math.ceil(totalCount / 20)}
            page={page}
            onChange={(_, v) => setPage(v)}
            color="primary"
          />
        </Box>
      </Container>
    </Box>
  );
};

export default Dashboard;
