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
} from '@mui/material';
import {
  Visibility as ViewIcon,
  Logout as LogoutIcon,
  Refresh as RefreshIcon,
} from '@mui/icons-material';
import { getQueue, Message } from '../services/api';
import { useAuth } from '../services/AuthContext';
import { format } from 'date-fns';

const Dashboard: React.FC = () => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [totalCount, setTotalCount] = useState(0);
  const [scoreRange, setScoreRange] = useState<number[]>([0, 100]);
  const [sortBy, setSortBy] = useState<string>('time');
  const [activeTab, setActiveTab] = useState<'pending' | 'reviewed'>('pending');
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

  const handleTabChange = (_: React.SyntheticEvent, newValue: 'pending' | 'reviewed') => {
    setActiveTab(newValue);
    setPage(1);
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
          <Button 
            color="inherit" 
            onClick={handlePullMessages} 
            startIcon={<CloudDownloadIcon />} 
            sx={{ mr: 2 }}
            disabled={ingesting}
          >
            {ingesting ? 'Pulling...' : 'Pull Messages'}
          </Button>
          <Button color="inherit" onClick={fetchMessages} startIcon={<RefreshIcon />} sx={{ mr: 2 }}>
            Refresh
          </Button>
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
        </Paper>

        {loading && <LinearProgress />}

        <TableContainer component={Paper}>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>ID</TableCell>
                <TableCell>Message Preview</TableCell>
                <TableCell>Group</TableCell>
                <TableCell>Timestamp</TableCell>
                <TableCell>Score</TableCell>
                <TableCell>Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {messages.map((msg) => (
                <TableRow key={msg.id} hover>
                  <TableCell>{msg.id}</TableCell>
                  <TableCell sx={{ maxWidth: 400 }}>
                    <Typography variant="body2" noWrap>
                      {msg.processed_message?.substring(0, 120) || msg.original_message?.substring(0, 120)}...
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2" noWrap>
                      {msg.group_name || msg.group_id?.substring(0, 8)}
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
                  </TableCell>
                </TableRow>
              ))}
              {messages.length === 0 && !loading && (
                <TableRow>
                  <TableCell colSpan={6} align="center">
                    <Typography color="text.secondary" sx={{ py: 4 }}>
                      No messages found
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
