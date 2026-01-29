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
  building_name: string | null;
  group_id: string;
  group_name: string | null;
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
  page: number;
  per_page: number;
}

export const getQueue = async (
  page: number = 1,
  status: string = 'pending'
): Promise<ModerationQueueResponse> => {
  const response = await api.get('/moderation/queue', {
    params: { page, status, per_page: 20 }
  });
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

export default api;
