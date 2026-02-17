
export interface Event {
  id?: number;
  event_id?: number;
  title: string;
  description?: string;
  starts_at: string;
  venue_id?: number;
  tags?: string[];
  my_rating?: number;
  score_pct?: number;
  gnn_score?: number;
  liked_at?: string;
  attended_at?: string;
}

export interface Venue {
  id: number;
  name: string;
  city?: string;
}

export interface User {
  id: number;
  name: string;
  email: string;
  city?: string;
}

export interface Friend {
  id: number;
  name: string;
  city?: string;
  friendship_status?: string;
}

export interface DateInfo {
  full: string;
  day: number;
  month: string;
  time: string;
  relative: string;
  dayName: string;
  isPast: boolean;
  isToday: boolean;
  isTomorrow: boolean;
}

export interface Filters {
  start: string;
  end: string;
  city: string;
  tags: string;
  search: string;
  sortBy?: string;
}

export interface LoginForm {
  email: string;
  password: string;
}

export interface VenuesMap {
  [key: number]: Venue;
}

export interface EventsByDate {
  [key: number]: Event[];
}
