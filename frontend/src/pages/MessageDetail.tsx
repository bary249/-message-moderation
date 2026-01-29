import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  Box,
  Container,
  Typography,
  Paper,
  Grid,
  Chip,
  Button,
  TextField,
  LinearProgress,
  AppBar,
  Toolbar,
  IconButton,
  Card,
  CardContent,
  Divider,
} from '@mui/material';
import {
  ArrowBack as BackIcon,
  CheckCircle as ReviewedIcon,
} from '@mui/icons-material';
import { getMessage, reviewMessage, Message } from '../services/api';
import { format } from 'date-fns';

const ScoreBar: React.FC<{ label: string; score: number }> = ({ label, score }) => {
  const getColor = (s: number) => {
    if (s < 0.3) return 'success';
    if (s < 0.6) return 'warning';
    return 'error';
  };

  return (
    <Box sx={{ mb: 2 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
        <Typography variant="body2">{label}</Typography>
        <Typography variant="body2" fontWeight="bold">
          {(score * 100).toFixed(0)}%
        </Typography>
      </Box>
      <LinearProgress
        variant="determinate"
        value={score * 100}
        color={getColor(score)}
        sx={{ height: 8, borderRadius: 4 }}
      />
    </Box>
  );
};

const MessageDetail: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [message, setMessage] = useState<Message | null>(null);
  const [loading, setLoading] = useState(true);
  const [reasoning, setReasoning] = useState('');
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    const fetchMessage = async () => {
      if (!id) return;
      try {
        const data = await getMessage(parseInt(id));
        setMessage(data);
      } catch (error) {
        console.error('Failed to fetch message:', error);
      }
      setLoading(false);
    };
    fetchMessage();
  }, [id]);

  const handleMarkReviewed = async () => {
    if (!id) return;
    setSubmitting(true);
    try {
      await reviewMessage(parseInt(id), 'reviewed', reasoning);
      navigate('/');
    } catch (error) {
      console.error('Failed to mark as reviewed:', error);
    }
    setSubmitting(false);
  };

  if (loading) {
    return <LinearProgress />;
  }

  if (!message) {
    return (
      <Container>
        <Typography>Message not found</Typography>
      </Container>
    );
  }

  return (
    <Box sx={{ flexGrow: 1 }}>
      <AppBar position="static">
        <Toolbar>
          <IconButton
            edge="start"
            color="inherit"
            onClick={() => navigate('/')}
            sx={{ mr: 2 }}
          >
            <BackIcon />
          </IconButton>
          <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
            Message #{message.id}
          </Typography>
        </Toolbar>
      </AppBar>

      <Container maxWidth="lg" sx={{ mt: 3 }}>
        <Grid container spacing={3}>
          {/* Message Content */}
          <Grid item xs={12} md={8}>
            <Paper sx={{ p: 3 }}>
              <Typography variant="h6" gutterBottom>
                Message Content (PII Removed)
              </Typography>
              <Paper
                variant="outlined"
                sx={{ p: 2, bgcolor: 'grey.50', whiteSpace: 'pre-wrap' }}
              >
                <Typography variant="body1">{message.processed_message}</Typography>
              </Paper>

              <Divider sx={{ my: 3 }} />

              <Typography variant="h6" gutterBottom>
                Metadata
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={6} sm={3}>
                  <Typography variant="caption" color="text.secondary">
                    Building
                  </Typography>
                  <Typography variant="body2" fontWeight="bold">
                    {message.building_name || message.building_id?.substring(0, 8)}
                  </Typography>
                </Grid>
                <Grid item xs={6} sm={3}>
                  <Typography variant="caption" color="text.secondary">
                    Group
                  </Typography>
                  <Typography variant="body2" fontWeight="bold">
                    {message.group_name || message.group_id?.substring(0, 8)}
                  </Typography>
                </Grid>
                <Grid item xs={6} sm={3}>
                  <Typography variant="caption" color="text.secondary">
                    Sender ID
                  </Typography>
                  <Typography variant="body2" fontWeight="bold">
                    {message.sender_id}
                  </Typography>
                </Grid>
                <Grid item xs={6} sm={3}>
                  <Typography variant="caption" color="text.secondary">
                    Message Time
                  </Typography>
                  <Typography variant="body2" fontWeight="bold">
                    {message.message_timestamp 
                      ? format(new Date(message.message_timestamp), 'MMM d, yyyy HH:mm')
                      : 'N/A'
                    }
                  </Typography>
                </Grid>
              </Grid>

              <Divider sx={{ my: 3 }} />

              {/* Review Actions */}
              {!message.is_reviewed && (
                <>
                  <Typography variant="h6" gutterBottom>
                    Review
                  </Typography>
                  <TextField
                    fullWidth
                    multiline
                    rows={3}
                    label="Notes (optional)"
                    value={reasoning}
                    onChange={(e) => setReasoning(e.target.value)}
                    sx={{ mb: 2 }}
                  />
                  <Button
                    variant="contained"
                    color="primary"
                    startIcon={<ReviewedIcon />}
                    onClick={handleMarkReviewed}
                    disabled={submitting}
                  >
                    Mark as Reviewed
                  </Button>
                </>
              )}

              {message.is_reviewed && (
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                  <Typography variant="h6">Status:</Typography>
                  <Chip label="Reviewed" color="success" />
                  {message.reviewed_at && (
                    <Typography variant="body2" color="text.secondary">
                      Reviewed: {format(new Date(message.reviewed_at), 'MMM d, yyyy HH:mm')}
                    </Typography>
                  )}
                </Box>
              )}
            </Paper>
          </Grid>

          {/* Moderation Scores */}
          <Grid item xs={12} md={4}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  AI Moderation Scores
                </Typography>
                <Box sx={{ mb: 3 }}>
                  <Typography variant="caption" color="text.secondary">
                    Overall Risk Score
                  </Typography>
                  <Typography variant="h3" fontWeight="bold" color={
                    message.moderation_score < 0.3 ? 'success.main' :
                    message.moderation_score < 0.6 ? 'warning.main' : 'error.main'
                  }>
                    {(message.moderation_score * 100).toFixed(0)}%
                  </Typography>
                </Box>

                <Divider sx={{ my: 2 }} />

                <ScoreBar label="Adversity / Hostility" score={message.adversity_score} />
                <ScoreBar label="Violence" score={message.violence_score} />
                <ScoreBar label="Inappropriate Content" score={message.inappropriate_content_score} />
                <ScoreBar label="Spam" score={message.spam_score} />

              </CardContent>
            </Card>
          </Grid>
        </Grid>
      </Container>
    </Box>
  );
};

export default MessageDetail;
