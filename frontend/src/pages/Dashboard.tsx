import React, { useState, useEffect, useCallback } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
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
} from '@mui/icons-material';
import { getQueue, reviewMessage, fetchMessagesByDate, scoreStream, clearAllMessages, removeDuplicates, Message, ScoredMessageEvent } from '../services/api';
import { useAuth } from '../services/AuthContext';
import { format } from 'date-fns';

const Dashboard: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [messages, setMessages] = useState<Message[]>([]);
  const [loading, setLoading] = useState(true);
  const [totalCount, setTotalCount] = useState(0);
  
  // Initialize state from URL params (persist filters across navigation)
  const [scoreRange, setScoreRange] = useState<number[]>(() => {
    const min = searchParams.get('scoreMin');
    const max = searchParams.get('scoreMax');
    return [min ? parseInt(min) : 0, max ? parseInt(max) : 100];
  });
  const [sortBy, setSortBy] = useState<string>(() => searchParams.get('sort') || 'time');
  const [activeTab, setActiveTab] = useState<'pending' | 'reviewed'>(() => 
    (searchParams.get('tab') as 'pending' | 'reviewed') || 'pending'
  );
  const [selectedIds, setSelectedIds] = useState<number[]>([]);
  const [selectedDate, setSelectedDate] = useState<string>('');
  const [fetching, setFetching] = useState(false);
  const [unscoredCount, setUnscoredCount] = useState<number | null>(null);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [scoringStatus, setScoringStatus] = useState<string>('idle');
  const { user, logout } = useAuth();
  const navigate = useNavigate();

  // Update URL params when filters change
  useEffect(() => {
    const params: Record<string, string> = {};
    if (scoreRange[0] !== 0) params.scoreMin = String(scoreRange[0]);
    if (scoreRange[1] !== 100) params.scoreMax = String(scoreRange[1]);
    if (sortBy !== 'time') params.sort = sortBy;
    if (activeTab !== 'pending') params.tab = activeTab;
    setSearchParams(params, { replace: true });
  }, [scoreRange, sortBy, activeTab, setSearchParams]);

  const fetchMessages = useCallback(async () => {
    setLoading(true);
    try {
      // Server-side sorting and filtering
      const data = await getQueue(
        1,  // Always page 1
        activeTab, 
        sortBy === 'time' ? 'time_desc' : sortBy,
        scoreRange[0] / 100,
        scoreRange[1] / 100,
        1000  // Load up to 1000 messages
      );
      
      setMessages(data.pending_messages);
      setTotalCount(data.total_count);
      setUnscoredCount(data.unscored_count);
    } catch (error) {
      console.error('Failed to fetch messages:', error);
    }
    setLoading(false);
  }, [scoreRange, sortBy, activeTab]);

  const [clearing, setClearing] = useState(false);

  const handleClearAll = async () => {
    if (!window.confirm('Are you sure you want to delete ALL messages from the local database? This cannot be undone.')) {
      return;
    }
    setClearing(true);
    try {
      const result = await clearAllMessages();
      setStatusMessage(`Cleared ${result.deleted_count} messages. ${result.message}`);
      setMessages([]);
      setTotalCount(0);
    } catch (error) {
      console.error('Failed to clear messages:', error);
      setStatusMessage('Failed to clear messages');
    }
    setClearing(false);
  };

  const handleRemoveDuplicates = async () => {
    try {
      const result = await removeDuplicates();
      setStatusMessage(`Removed ${result.removed} duplicates. ${result.remaining} messages remaining.`);
      fetchMessages();
    } catch (error) {
      console.error('Failed to remove duplicates:', error);
      setStatusMessage('Failed to remove duplicates');
    }
  };

  useEffect(() => {
    fetchMessages();
  }, [fetchMessages]);

  const handleScoreChange = (_: Event, newValue: number | number[]) => {
    setScoreRange(newValue as number[]);
  };

  const handleSortChange = (event: SelectChangeEvent) => {
    setSortBy(event.target.value);
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

    // Score stream disabled - was causing connection issues
  // useEffect(() => {
  //   const eventSource = scoreStream({
  //     onScored: (data: ScoredMessageEvent) => {
  //       setMessages(prev => prev.map(msg => 
  //         msg.id === data.message_id 
  //           ? { ...msg, ...data }
  //           : msg
  //       ));
  //       setUnscoredCount(prev => prev ? prev - 1 : 0);
  //       setScoringStatus('scoring');
  //     },
  //     onWaiting: () => setScoringStatus('waiting')
  //   });
  //   return () => eventSource.close();
  // }, []);

  const handleTabChange = (_: React.SyntheticEvent, newValue: 'pending' | 'reviewed') => {
    setActiveTab(newValue);
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
          {scoringStatus === 'scoring' && (
            <Chip 
              label="Auto-scoring messages..." 
              color="info" 
              variant="outlined"
              size="small"
            />
          )}
          {scoringStatus === 'waiting' && (
            <Chip 
              label="Waiting for messages to score" 
              color="default" 
              variant="outlined"
              size="small"
            />
          )}
          <Button
            variant="outlined"
            color="warning"
            onClick={handleRemoveDuplicates}
            disabled={totalCount === 0}
            size="small"
          >
            Remove Duplicates
          </Button>
          <Button
            variant="outlined"
            color="error"
            onClick={handleClearAll}
            disabled={clearing || totalCount === 0}
            size="small"
          >
            {clearing ? 'Clearing...' : 'Clear All'}
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
                      label={msg.moderation_score !== null ? `${(msg.moderation_score * 100).toFixed(0)}%` : 'Unscored'}
                      color={msg.moderation_score !== null ? getScoreColor(msg.moderation_score) : 'default'}
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

        {/* All messages loaded - no pagination needed */}
      </Container>
    </Box>
  );
};

export default Dashboard;
