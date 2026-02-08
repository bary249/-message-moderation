import axios from 'axios';

// Use environment variable for production, fallback to proxy for development
const API_BASE_URL = process.env.REACT_APP_API_URL || '';

const api = axios.create({
  baseURL: `${API_BASE_URL}/api/v1`,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Add auth token to requests
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// Handle auth errors
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('token');
      localStorage.removeItem('user');
      window.location.href = '/login';
    }
    return Promise.reject(error);
  }
);

export interface Message {
  id: number;
  original_message: string;
  processed_message: string;
  building_id: string;
  building_name?: string;
  client_name?: string;
  group_id: string;
  group_name?: string | null;
  sender_id: string;
  message_timestamp: string | null;
  timestamp: string;
  moderation_score: number;
  is_reviewed: boolean;
  adversity_score: number;
  violence_score: number;
  inappropriate_content_score: number;
  spam_score: number;
  created_at: string;
  reviewed_at: string | null;
}

export interface ModerationQueueResponse {
  pending_messages: Message[];
  total_count: number;
  unscored_count: number;
  page: number;
  per_page: number;
}

export const getQueue = async (
  page: number = 1,
  status: string = 'pending',
  sortBy: string = 'time_desc',
  scoreMin?: number,
  scoreMax?: number,
  perPage: number = 50,
  clientName?: string
): Promise<ModerationQueueResponse> => {
  const response = await api.get('/moderation/queue', {
    params: { 
      page, 
      status, 
      per_page: perPage,
      sort_by: sortBy,
      score_min: scoreMin,
      score_max: scoreMax,
      client_name: clientName || undefined
    }
  });
  return response.data;
};

export const getClients = async (): Promise<{clients: string[]}> => {
  const response = await api.get('/moderation/clients');
  return response.data;
};

export const getMessage = async (id: number): Promise<Message> => {
  const response = await api.get(`/messages/${id}`);
  return response.data;
};

export const reviewMessage = async (
  messageId: number,
  action: 'reviewed',
  reasoning?: string
): Promise<void> => {
  await api.post(`/moderation/review/${messageId}`, {
    action,
    reasoning
  });
};

export interface IngestResponse {
  ingested_count: number;
  total_fetched: number;
  results: Array<{
    message_id: number;
    status: string;
    moderation_score: number;
    group_name: string;
    text_preview: string;
  }>;
}

export const ingestMessages = async (
  limit: number = 20,
  days_back: number = 1
): Promise<IngestResponse> => {
  const response = await api.post('/snowflake/ingest', null, {
    params: { limit, days_back }
  });
  return response.data;
};

export interface FetchByDateResponse {
  status: string;
  fetched_from_snowflake: number;
  new_messages_saved: number;
  target_date: string;
}

export const fetchMessagesByDate = async (targetDate: string): Promise<FetchByDateResponse> => {
  const response = await api.post('/snowflake/fetch-by-date', null, {
    params: { target_date: targetDate }
  });
  return response.data;
};

export interface FetchByDateRangeResponse {
  status: string;
  fetched_from_snowflake: number;
  new_messages_saved: number;
  start_date: string;
  end_date: string;
}

export const fetchMessagesByDateRange = async (startDate: string, endDate: string): Promise<FetchByDateRangeResponse> => {
  const response = await api.post('/snowflake/fetch-by-date-range', null, {
    params: { start_date: startDate, end_date: endDate }
  });
  return response.data;
};

export interface ScoreBatchResponse {
  status: string;
  scored: number;
  remaining: number;
  elapsed_seconds?: number;
  message?: string;
}

export const scoreBatch = async (limit: number = 20): Promise<ScoreBatchResponse> => {
  const response = await api.post('/moderation/score-batch', null, {
    params: { limit }
  });
  return response.data;
};

export interface ScoredMessageEvent {
  message_id: number;
  moderation_score: number;
  adversity_score: number;
  violence_score: number;
  inappropriate_content_score: number;
  spam_score: number;
  processed_message: string;
}

export interface ScoreStreamCallbacks {
  onScored?: (data: ScoredMessageEvent) => void;
  onWaiting?: (data: { status: string; message: string }) => void;
}

export const scoreStream = (callbacks: ScoreStreamCallbacks): EventSource => {
  const baseUrl = process.env.REACT_APP_API_URL || '';
  const url = `${baseUrl}/api/v1/moderation/score-stream`;
  
  const eventSource = new EventSource(url);
  
  eventSource.addEventListener('scored', (e: MessageEvent) => {
    const data = JSON.parse(e.data);
    callbacks.onScored?.(data);
  });
  
  eventSource.addEventListener('waiting', (e: MessageEvent) => {
    const data = JSON.parse(e.data);
    callbacks.onWaiting?.(data);
  });
  
  eventSource.onerror = () => {
    eventSource.close();
  };
  
  return eventSource;
};

export const clearAllMessages = async (): Promise<{ status: string; deleted_count: number; message: string }> => {
  const response = await api.delete('/moderation/clear-all');
  return response.data;
};

export const removeDuplicates = async (): Promise<{ status: string; removed: number; remaining: number; message: string }> => {
  const response = await api.delete('/moderation/remove-duplicates');
  return response.data;
};

export default api;
