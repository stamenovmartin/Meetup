import React, { useState, useEffect, useCallback, useMemo } from 'react';
import {
  Calendar, MapPin, Users, Heart, Search, User, Clock, Bell,
  CheckCircle, LogOut, Filter, LayoutGrid, ChevronLeft, ChevronRight,
  Activity, Sparkles, ArrowRight, X, UserPlus, Loader2
} from 'lucide-react';
import type { Event, Venue, User as AppUser, Friend, DateInfo, LoginForm, VenuesMap } from './types';

const BACKEND_BASE = import.meta.env.VITE_BACKEND_URL || "http://127.0.0.1:5000";

// --- HELPERS ---
const getStoredTheme = (): boolean => {
  try { return JSON.parse(window.localStorage.getItem('darkMode') || 'false'); } catch { return false; }
};

const setStoredTheme = (isDark: boolean): void => {
  try { window.localStorage.setItem('darkMode', JSON.stringify(isDark)); } catch {}
};

const getStoredToken = (): string | null => {
  try { return window.localStorage.getItem('access_token'); } catch { return null; }
};

const setStoredToken = (token: string | null): void => {
  try {
    if (token) window.localStorage.setItem('access_token', token);
    else window.localStorage.removeItem('access_token');
  } catch {}
};

const formatEventDate = (dateStr: string): DateInfo => {
  try {
    const date = new Date(dateStr);
    const now = new Date();
    const diffTime = date.getTime() - now.getTime();
    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));

    const dayNames = ['Недела', 'Понеделник', 'Вторник', 'Среда', 'Четврток', 'Петок', 'Сабота'];
    const monthNames = ['јан', 'фев', 'мар', 'апр', 'мај', 'јун', 'јул', 'авг', 'сеп', 'окт', 'ное', 'дек'];

    return {
      full: `${dayNames[date.getDay()]}, ${date.getDate()} ${monthNames[date.getMonth()]}`,
      day: date.getDate(),
      month: monthNames[date.getMonth()],
      time: date.toLocaleTimeString('mk-MK', { hour: '2-digit', minute: '2-digit' }),
      relative: diffDays === 0 ? 'Денес' : diffDays === 1 ? 'Утре' : diffDays < 0 ? `Пред ${Math.abs(diffDays)} дена` : `За ${diffDays} дена`,
      dayName: dayNames[date.getDay()].substring(0, 3),
      isPast: diffDays < 0,
      isToday: diffDays === 0,
      isTomorrow: diffDays === 1
    };
  } catch {
    return { full: '?', day: 0, month: '?', time: '?', relative: '', dayName: '?', isPast: false, isToday: false, isTomorrow: false };
  }
};

// --- COMPONENTS ---

// Simple Toast Notification
const Toast: React.FC<{ message: string; type: 'success' | 'error'; onClose: () => void }> = ({ message, type, onClose }) => {
  useEffect(() => {
    const timer = setTimeout(onClose, 3000);
    return () => clearTimeout(timer);
  }, [onClose]);

  return (
    <div
      className={`fixed bottom-6 right-6 z-50 px-6 py-4 rounded-2xl shadow-2xl flex items-center gap-3 animate-fade-in ${
        type === 'success'
          ? 'bg-emerald-500 text-white'
          : 'bg-rose-500 text-white'
      }`}
    >
      {type === 'success' ? (
        <CheckCircle className="w-5 h-5" />
      ) : (
        <X className="w-5 h-5" />
      )}
      <span className="font-bold">{message}</span>
      <button onClick={onClose} className="ml-2 hover:opacity-70">
        <X className="w-4 h-4" />
      </button>
    </div>
  );
};

const GlassCard: React.FC<{ children: React.ReactNode; className?: string }> = ({ children, className = '' }) => (
  <div className={`bg-white/80 dark:bg-slate-900/80 backdrop-blur-xl rounded-3xl shadow-sm border border-slate-200/50 dark:border-slate-700/50 transition-all duration-500 hover:shadow-xl hover:shadow-indigo-500/10 ${className}`}>
    {children}
  </div>
);

const EventCard: React.FC<{
  event: Event;
  venue?: Venue;
  onToggleFavorite?: (eventId: number) => Promise<void>;
  onToggleAttended?: (eventId: number) => Promise<void>;
  showRecommendation?: boolean;
}> = ({ event, venue, onToggleFavorite, onToggleAttended, showRecommendation }) => {
  const [likeLoading, setLikeLoading] = useState(false);
  const [attendLoading, setAttendLoading] = useState(false);
  const [toast, setToast] = useState<{ message: string; type: 'success' | 'error' } | null>(null);
  const dateInfo = formatEventDate(event.starts_at);
  const tags = event.tags || [];

  const handleLike = async () => {
    const id = event.id || event.event_id;
    if (!id || likeLoading || !onToggleFavorite) return;
    setLikeLoading(true);
    try {
      await onToggleFavorite(id);
      setToast({ message: isLiked ? '❤️ Отстрането од омилени' : '✅ Додадено во омилени', type: 'success' });
    } catch (err) {
      setToast({ message: '❌ Грешка при зачувување', type: 'error' });
      console.error(err);
    } finally {
      setLikeLoading(false);
    }
  };

  const handleAttend = async () => {
    const id = event.id || event.event_id;
    if (!id || attendLoading || !onToggleAttended) return;
    setAttendLoading(true);
    try {
      await onToggleAttended(id);
      setToast({ message: '✅ Статусот е ажуриран', type: 'success' });
    } catch (err) {
      setToast({ message: '❌ Грешка при ажурирање', type: 'error' });
      console.error(err);
    } finally {
      setAttendLoading(false);
    }
  };

  const isLiked = event.my_rating === 1;

  const isLoading = likeLoading || attendLoading;

  return (
    <>
      {toast && <Toast message={toast.message} type={toast.type} onClose={() => setToast(null)} />}
      <GlassCard className="group overflow-hidden flex flex-col h-full border-b-4 border-b-transparent hover:border-b-indigo-500 relative">
        {/* Loading Overlay */}
        {isLoading && (
          <div className="absolute inset-0 bg-white/80 dark:bg-slate-900/80 backdrop-blur-sm z-10 flex items-center justify-center rounded-3xl">
            <div className="flex flex-col items-center gap-3">
              <Loader2 className="w-12 h-12 text-indigo-600 dark:text-indigo-400 animate-spin" />
              <span className="text-sm font-bold text-slate-600 dark:text-slate-300 animate-pulse">
                Се зачувува...
              </span>
            </div>
          </div>
        )}
        <div className="relative h-52 overflow-hidden">
        <img
          src={`https://picsum.photos/seed/${event.id || event.event_id}/800/600`}
          alt={event.title}
          className="w-full h-full object-cover transition-transform duration-700 group-hover:scale-110"
        />
        <div className="absolute inset-0 bg-gradient-to-t from-slate-900/80 via-transparent to-transparent opacity-60"></div>

        <div className="absolute top-4 left-4">
          <div className="bg-white/90 backdrop-blur-md dark:bg-slate-800/90 rounded-2xl p-2 px-3 shadow-2xl text-center min-w-[50px]">
            <span className="block text-xl font-black text-indigo-600 dark:text-indigo-400 leading-none">{dateInfo.day}</span>
            <span className="block text-[10px] uppercase font-black tracking-tighter text-slate-500">{dateInfo.month}</span>
          </div>
        </div>

        {showRecommendation && typeof event.gnn_score === 'number' && (
          <div className="absolute top-4 right-4 bg-indigo-600 text-white px-3 py-1.5 rounded-full text-[10px] font-black shadow-lg flex items-center gap-1.5 animate-pulse">
            <Sparkles className="w-3 h-3" />
            {event.gnn_score.toFixed(1)}% GNN
          </div>
        )}
      </div>

      <div className="p-6 flex-1 flex flex-col">
        <div className="flex-1">
          <h3 className="text-xl font-bold text-slate-900 dark:text-white mb-3 line-clamp-2 group-hover:text-indigo-600 transition-colors">
            {event.title}
          </h3>

          <div className="space-y-2.5 mb-5">
            <div className="flex items-center text-slate-500 dark:text-slate-400 text-sm font-medium">
              <MapPin className="w-4 h-4 mr-2 text-indigo-500" />
              <span className="truncate">{venue?.name || 'Локација'}</span>
            </div>
            <div className="flex items-center text-slate-500 dark:text-slate-400 text-sm font-medium">
              <Clock className="w-4 h-4 mr-2 text-indigo-500" />
              <span>{dateInfo.time} • {dateInfo.relative}</span>
            </div>
          </div>

          <div className="flex flex-wrap gap-2 mb-6">
            {tags.slice(0, 3).map((tag, i) => (
              <span key={i} className="px-2.5 py-1 bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-400 rounded-lg text-[10px] font-black uppercase tracking-widest border border-slate-200/50 dark:border-slate-700/50">
                {tag}
              </span>
            ))}
          </div>
        </div>

        <div className="pt-5 border-t border-slate-100 dark:border-slate-800 flex items-center justify-between">
          <div className="flex gap-2">
            <button
              onClick={handleLike}
              disabled={likeLoading}
              className={`relative p-2.5 rounded-2xl transition-all active:scale-95 ${
                isLiked
                  ? 'bg-rose-500 text-white shadow-lg shadow-rose-500/30'
                  : 'hover:bg-slate-100 dark:hover:bg-slate-800 text-slate-400'
              } ${likeLoading ? 'cursor-wait' : ''}`}
            >
              {likeLoading ? (
                <Loader2 className="w-5 h-5 animate-spin" />
              ) : (
                <Heart className={`w-5 h-5 ${isLiked ? 'fill-current' : ''}`} />
              )}
            </button>
            <button
              onClick={handleAttend}
              disabled={attendLoading}
              className={`relative p-2.5 rounded-2xl hover:bg-emerald-50 dark:hover:bg-emerald-900/20 text-slate-400 hover:text-emerald-500 transition-all active:scale-95 ${
                attendLoading ? 'cursor-wait' : ''
              }`}
            >
              {attendLoading ? (
                <Loader2 className="w-5 h-5 animate-spin" />
              ) : (
                <CheckCircle className="w-5 h-5" />
              )}
            </button>
          </div>
          <button className="flex items-center gap-1.5 text-indigo-600 dark:text-indigo-400 font-black text-xs uppercase tracking-widest hover:gap-2.5 transition-all active:scale-95">
            Детали <ArrowRight className="w-4 h-4" />
          </button>
        </div>
      </div>
    </GlassCard>
    </>
  );
};

// Calendar View Component
const CalendarView: React.FC<{ events: Event[]; venues: VenuesMap }> = ({ events, venues }) => {
  const [currentDate, setCurrentDate] = useState(new Date());
  const [selectedDate, setSelectedDate] = useState<number | null>(null);

  const daysInMonth = new Date(currentDate.getFullYear(), currentDate.getMonth() + 1, 0).getDate();
  const firstDay = new Date(currentDate.getFullYear(), currentDate.getMonth(), 1).getDay();
  const startingDayOfWeek = firstDay === 0 ? 6 : firstDay - 1;

  const eventsByDate = useMemo(() => {
    const grouped: { [key: number]: Event[] } = {};
    events.forEach(event => {
      const eventDate = new Date(event.starts_at);
      if (eventDate.getMonth() === currentDate.getMonth() && eventDate.getFullYear() === currentDate.getFullYear()) {
        const date = eventDate.getDate();
        if (!grouped[date]) grouped[date] = [];
        grouped[date].push(event);
      }
    });
    return grouped;
  }, [events, currentDate]);

  const monthNames = ['Јануари', 'Февруари', 'Март', 'Април', 'Мај', 'Јуни', 'Јули', 'Август', 'Септември', 'Октомври', 'Ноември', 'Декември'];
  const dayNames = ['Пон', 'Вто', 'Сре', 'Чет', 'Пет', 'Саб', 'Нед'];

  return (
    <div className="space-y-6">
      <GlassCard className="p-6">
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-2xl font-bold text-slate-900 dark:text-white">
            {monthNames[currentDate.getMonth()]} {currentDate.getFullYear()}
          </h2>
          <div className="flex items-center gap-3">
            <button
              onClick={() => setCurrentDate(new Date())}
              className="px-4 py-2 text-sm font-black bg-indigo-50 dark:bg-indigo-900/20 text-indigo-600 rounded-xl hover:bg-indigo-100 transition"
            >
              Денес
            </button>
            <button
              onClick={() => setCurrentDate(new Date(currentDate.getFullYear(), currentDate.getMonth() - 1))}
              className="p-2 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-xl transition"
            >
              <ChevronLeft className="w-5 h-5" />
            </button>
            <button
              onClick={() => setCurrentDate(new Date(currentDate.getFullYear(), currentDate.getMonth() + 1))}
              className="p-2 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-xl transition"
            >
              <ChevronRight className="w-5 h-5" />
            </button>
          </div>
        </div>

        <div className="grid grid-cols-7 gap-2">
          {dayNames.map(day => (
            <div key={day} className="p-3 text-center text-xs font-black text-slate-500 dark:text-slate-400 uppercase tracking-widest">
              {day}
            </div>
          ))}

          {Array.from({ length: startingDayOfWeek }).map((_, i) => (
            <div key={`empty-${i}`} className="p-1 h-20"></div>
          ))}

          {Array.from({ length: daysInMonth }).map((_, i) => {
            const day = i + 1;
            const dayEvents = eventsByDate[day] || [];
            const isToday = new Date().toDateString() === new Date(currentDate.getFullYear(), currentDate.getMonth(), day).toDateString();

            return (
              <div
                key={day}
                onClick={() => setSelectedDate(day)}
                className={`p-2 h-20 rounded-xl border-2 cursor-pointer transition-all ${
                  dayEvents.length > 0
                    ? 'bg-indigo-50 dark:bg-indigo-900/20 border-indigo-200 dark:border-indigo-800 hover:border-indigo-400'
                    : 'border-slate-200/50 dark:border-slate-800/50 hover:bg-slate-50 dark:hover:bg-slate-900'
                } ${isToday ? 'ring-2 ring-indigo-500' : ''} ${selectedDate === day ? 'ring-2 ring-indigo-600 bg-indigo-100 dark:bg-indigo-900/40' : ''}`}
              >
                <div className={`text-sm font-bold mb-1 ${isToday ? 'text-indigo-600' : 'text-slate-700 dark:text-slate-300'}`}>
                  {day}
                </div>
                {dayEvents.length > 0 && (
                  <div className="text-[10px] font-black text-indigo-600 dark:text-indigo-400">
                    {dayEvents.length} настан{dayEvents.length > 1 ? 'и' : ''}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </GlassCard>

      {selectedDate && eventsByDate[selectedDate] && (
        <GlassCard className="p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-xl font-bold text-slate-900 dark:text-white">
              Настани за {selectedDate} {monthNames[currentDate.getMonth()]}
            </h3>
            <button onClick={() => setSelectedDate(null)} className="text-slate-400 hover:text-slate-600 transition">
              <X className="w-5 h-5" />
            </button>
          </div>
          <div className="space-y-4">
            {eventsByDate[selectedDate].map(event => (
              <div key={event.id || event.event_id} className="border-l-4 border-indigo-500 pl-4 py-2">
                <h4 className="font-bold text-slate-900 dark:text-white">{event.title}</h4>
                <div className="flex items-center text-sm text-slate-500 dark:text-slate-400 mt-1">
                  <Clock className="w-4 h-4 mr-2" />
                  <span>{formatEventDate(event.starts_at).time}</span>
                  {event.venue_id && venues[event.venue_id] && (
                    <>
                      <span className="mx-2">•</span>
                      <MapPin className="w-4 h-4 mr-1" />
                      <span>{venues[event.venue_id].name}</span>
                    </>
                  )}
                </div>
              </div>
            ))}
          </div>
        </GlassCard>
      )}
    </div>
  );
};

// Search & Filter Component
const SearchComponent: React.FC<{
  onSearch: (query: string, filters: any) => void;
}> = ({ onSearch }) => {
  const [query, setQuery] = useState('');
  const [showFilters, setShowFilters] = useState(false);
  const [filters, setFilters] = useState<any>({});

  const handleSearch = () => {
    onSearch(query, filters);
  };

  return (
    <div className="space-y-4">
      <div className="relative group">
        <Search className="absolute left-6 top-1/2 -translate-y-1/2 w-6 h-6 text-slate-400 group-focus-within:text-indigo-600 transition" />
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyPress={(e) => e.key === 'Enter' && handleSearch()}
          placeholder="Кој е твојот следен план?"
          className="w-full bg-white dark:bg-slate-900 border-2 border-slate-200/50 dark:border-slate-800/50 rounded-[2rem] py-6 pl-16 pr-6 focus:outline-none focus:border-indigo-500 transition-all shadow-xl font-bold text-lg"
        />
        <button
          onClick={() => setShowFilters(!showFilters)}
          className="absolute right-4 top-1/2 -translate-y-1/2 bg-indigo-600 text-white p-3.5 rounded-2xl hover:scale-105 transition"
        >
          <Filter className="w-5 h-5" />
        </button>
      </div>

      {showFilters && (
        <GlassCard className="p-6 animate-in slide-in-from-top-2">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div>
              <label className="block text-xs font-black text-slate-400 uppercase tracking-widest mb-2">Од датум</label>
              <input
                type="date"
                value={filters.start || ''}
                onChange={(e) => setFilters({ ...filters, start: e.target.value })}
                className="w-full p-3 border-2 border-slate-200 dark:border-slate-700 rounded-xl bg-white dark:bg-slate-900 text-slate-900 dark:text-white focus:border-indigo-500 focus:outline-none"
              />
            </div>
            <div>
              <label className="block text-xs font-black text-slate-400 uppercase tracking-widest mb-2">До датум</label>
              <input
                type="date"
                value={filters.end || ''}
                onChange={(e) => setFilters({ ...filters, end: e.target.value })}
                className="w-full p-3 border-2 border-slate-200 dark:border-slate-700 rounded-xl bg-white dark:bg-slate-900 text-slate-900 dark:text-white focus:border-indigo-500 focus:outline-none"
              />
            </div>
            <div>
              <label className="block text-xs font-black text-slate-400 uppercase tracking-widest mb-2">Град</label>
              <input
                type="text"
                value={filters.city || ''}
                onChange={(e) => setFilters({ ...filters, city: e.target.value })}
                placeholder="Скопје, Битола..."
                className="w-full p-3 border-2 border-slate-200 dark:border-slate-700 rounded-xl bg-white dark:bg-slate-900 text-slate-900 dark:text-white focus:border-indigo-500 focus:outline-none"
              />
            </div>
          </div>
          <div className="flex justify-end gap-3 mt-4">
            <button
              onClick={() => setFilters({})}
              className="px-6 py-3 bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 rounded-xl font-black text-sm hover:bg-slate-200 dark:hover:bg-slate-700 transition"
            >
              Ресетирај
            </button>
            <button
              onClick={handleSearch}
              className="px-6 py-3 bg-indigo-600 text-white rounded-xl font-black text-sm hover:bg-indigo-700 transition"
            >
              Пребарај
            </button>
          </div>
        </GlassCard>
      )}
    </div>
  );
};

// Group Recommendations Component
const GroupRecommendations: React.FC<{
  friends: Friend[];
  onGetRecommendations: (selectedIds: number[]) => void;
  recommendations: Event[];
  venues: VenuesMap;
}> = ({ friends, onGetRecommendations, recommendations, venues }) => {
  const [selectedFriends, setSelectedFriends] = useState<number[]>([]);

  const toggleFriend = (id: number) => {
    setSelectedFriends(prev =>
      prev.includes(id) ? prev.filter(x => x !== id) : [...prev, id]
    );
  };

  return (
    <div className="space-y-6">
      <GlassCard className="p-6">
        <h3 className="text-xl font-bold text-slate-900 dark:text-white mb-4 flex items-center gap-3">
          <Users className="w-6 h-6 text-indigo-600" />
          Избери пријатели
        </h3>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
          {friends.map(friend => (
            <button
              key={friend.id}
              onClick={() => toggleFriend(friend.id)}
              className={`p-4 rounded-2xl border-2 transition-all ${
                selectedFriends.includes(friend.id)
                  ? 'bg-indigo-100 dark:bg-indigo-900/30 border-indigo-500 text-indigo-700 dark:text-indigo-300'
                  : 'border-slate-200 dark:border-slate-700 hover:border-indigo-300'
              }`}
            >
              <div className="text-sm font-bold">{friend.name}</div>
              <div className="text-[10px] text-slate-500 dark:text-slate-400">{friend.city}</div>
            </button>
          ))}
        </div>
        <button
          onClick={() => onGetRecommendations(selectedFriends)}
          disabled={selectedFriends.length === 0}
          className="w-full py-4 bg-indigo-600 text-white rounded-2xl font-black uppercase tracking-widest disabled:opacity-50 hover:bg-indigo-700 transition"
        >
          Најди заеднички настани ({selectedFriends.length})
        </button>
      </GlassCard>

      {recommendations.length > 0 && (
        <div>
          <h3 className="text-2xl font-bold text-slate-900 dark:text-white mb-6">
            Заеднички препораки
          </h3>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {recommendations.map(event => (
              <EventCard key={event.id || event.event_id} event={event} venue={venues[event.venue_id!]} showRecommendation />
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

// Notifications Component
const Notifications: React.FC<{ accessToken: string | null }> = ({ accessToken }) => {
  const [notifications, setNotifications] = useState<any[]>([]);
  const [showDropdown, setShowDropdown] = useState(false);

  useEffect(() => {
    if (accessToken) {
      fetch(`${BACKEND_BASE}/api/friends/pending`, {
        headers: { Authorization: `Bearer ${accessToken}` }
      })
        .then(res => res.json())
        .then(data => setNotifications(data || []))
        .catch(() => {});
    }
  }, [accessToken]);

  const acceptFriend = async (requesterId: number) => {
    try {
      await fetch(`${BACKEND_BASE}/api/friends/accept`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${accessToken}`
        },
        body: JSON.stringify({ user_id: requesterId })
      });
      setNotifications(prev => prev.filter(n => n.requester_id !== requesterId));
    } catch {}
  };

  return (
    <div className="relative">
      <button
        onClick={() => setShowDropdown(!showDropdown)}
        className="p-3 rounded-2xl bg-slate-100 dark:bg-slate-900 text-slate-600 dark:text-slate-400 hover:bg-slate-200 dark:hover:bg-slate-800 transition relative"
      >
        <Bell className="w-5 h-5" />
        {notifications.length > 0 && (
          <span className="absolute top-1 right-1 w-5 h-5 bg-rose-500 text-white text-[10px] font-black rounded-full flex items-center justify-center">
            {notifications.length}
          </span>
        )}
      </button>

      {showDropdown && (
        <div className="absolute right-0 mt-2 w-80 bg-white dark:bg-slate-800 rounded-2xl shadow-2xl border-2 border-slate-200/50 dark:border-slate-700/50 z-50 overflow-hidden">
          <div className="p-4 border-b border-slate-200 dark:border-slate-700">
            <h3 className="font-black text-slate-900 dark:text-white">Нотификации</h3>
          </div>
          <div className="max-h-96 overflow-y-auto">
            {notifications.length > 0 ? (
              notifications.map((notif, idx) => (
                <div key={idx} className="p-4 border-b border-slate-100 dark:border-slate-700/50 hover:bg-slate-50 dark:hover:bg-slate-700/30">
                  <div className="flex items-start gap-3">
                    <div className="w-10 h-10 bg-indigo-500 rounded-2xl flex items-center justify-center">
                      <UserPlus className="w-5 h-5 text-white" />
                    </div>
                    <div className="flex-1">
                      <p className="text-sm text-slate-900 dark:text-white font-medium">
                        <span className="font-black">{notif.requester_name}</span> сака да те додаде
                      </p>
                      <div className="flex gap-2 mt-2">
                        <button
                          onClick={() => acceptFriend(notif.requester_id)}
                          className="px-4 py-1.5 bg-indigo-600 text-white rounded-xl text-xs font-black hover:bg-indigo-700"
                        >
                          Прифати
                        </button>
                        <button
                          onClick={() => setShowDropdown(false)}
                          className="px-4 py-1.5 bg-slate-200 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-xl text-xs font-black"
                        >
                          Подоцна
                        </button>
                      </div>
                    </div>
                  </div>
                </div>
              ))
            ) : (
              <div className="p-8 text-center text-slate-400">Нема нови нотификации</div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

// --- MAIN APP ---
const EventSocialNetwork: React.FC = () => {
  const [activeTab, setActiveTab] = useState('discover');
  const [darkMode, setDarkMode] = useState(getStoredTheme());
  const [user, setUser] = useState<AppUser | null>(null);
  const [accessToken, setAccessToken] = useState<string | null>(getStoredToken());
  const [events, setEvents] = useState<Event[]>([]);
  const [venues, setVenues] = useState<VenuesMap>({});
  const [recommendations, setRecommendations] = useState<Event[]>([]);
  const [favorites, setFavorites] = useState<Event[]>([]);
  const [friends, setFriends] = useState<Friend[]>([]);
  const [feed, setFeed] = useState<any[]>([]);
  const [groupRecs, setGroupRecs] = useState<Event[]>([]);
  const [loading, setLoading] = useState(false);
  const [loginForm, setLoginForm] = useState<LoginForm>({ email: '', password: '' });
  const [showFindFriends, setShowFindFriends] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<any[]>([]);

  useEffect(() => {
    document.documentElement.classList.toggle('dark', darkMode);
    setStoredTheme(darkMode);
  }, [darkMode]);

  const apiCall = useCallback(async (endpoint: string, options: RequestInit = {}) => {
    const headers: any = { 'Content-Type': 'application/json' };
    if (accessToken) headers.Authorization = `Bearer ${accessToken}`;

    const response = await fetch(`${BACKEND_BASE}${endpoint}`, { headers, ...options });
    if (!response.ok) throw new Error();
    return await response.json();
  }, [accessToken]);

  const loadData = useCallback(async () => {
    if (!accessToken) return;
    setLoading(true);
    try {
      const [evs, vens, recs, favs, frnds, feedData] = await Promise.all([
        apiCall('/api/events'),
        apiCall('/api/venues'),
        apiCall('/api/recommend/me?limit=10000'),
        apiCall('/api/user/favorites'),
        apiCall('/api/friends/list'),
        apiCall('/api/feed')
      ]);
      setEvents(Array.isArray(evs) ? evs : []);
      setVenues((vens || []).reduce((acc: any, v: Venue) => ({ ...acc, [v.id]: v }), {}));
      setRecommendations(Array.isArray(recs) ? recs : []);
      setFavorites(Array.isArray(favs) ? favs : []);
      setFriends(Array.isArray(frnds) ? frnds : []);
      setFeed(Array.isArray(feedData) ? feedData : []);
    } catch (err) {
      console.error('Error loading data:', err);
    } finally {
      setLoading(false);
    }
  }, [accessToken, apiCall]);

  useEffect(() => {
    if (accessToken) loadData();
  }, [accessToken, loadData]);

  const handleLogin = async () => {
    if (!loginForm.email || !loginForm.password) return;
    setLoading(true);
    try {
      const data = await apiCall('/api/auth/login', {
        method: 'POST',
        body: JSON.stringify(loginForm)
      });
      setAccessToken(data.access_token);
      setStoredToken(data.access_token);
      setUser(data.user);
    } catch {
      alert('Погрешен email или лозинка!');
    } finally {
      setLoading(false);
    }
  };

  const handleToggleFavorite = async (eventId: number) => {
    try {
      const event = [...events, ...recommendations, ...favorites].find(e => (e.id || e.event_id) === eventId);
      await apiCall(`/api/events/${eventId}/rate`, {
        method: 'POST',
        body: JSON.stringify({ rating: event?.my_rating === 1 ? 0 : 1 })
      });
      await loadData();
    } catch {}
  };

  const handleToggleAttended = async (eventId: number) => {
    try {
      await apiCall(`/api/events/${eventId}/rate`, {
        method: 'POST',
        body: JSON.stringify({ rating: 0 })
      });
      await loadData();
    } catch {}
  };

  const handleGroupRecommendations = async (selectedIds: number[]) => {
    if (selectedIds.length === 0) return;
    setLoading(true);
    try {
      const data = await apiCall('/api/recommend/group', {
        method: 'POST',
        body: JSON.stringify({ user_ids: selectedIds })
      });
      setGroupRecs(Array.isArray(data) ? data : []);
    } catch {
      setGroupRecs([]);
    } finally {
      setLoading(false);
    }
  };

  const handleSearchUsers = async () => {
    if (!searchQuery.trim()) return;
    try {
      const data = await apiCall(`/api/users/search?q=${encodeURIComponent(searchQuery)}`);
      setSearchResults(Array.isArray(data) ? data : []);
    } catch {
      setSearchResults([]);
    }
  };

  const handleFriendRequest = async (userId: number) => {
    try {
      await apiCall('/api/friends/request', {
        method: 'POST',
        body: JSON.stringify({ user_id: userId })
      });
      await handleSearchUsers();
    } catch {}
  };

  if (!accessToken) {
    return (
      <div className="min-h-screen bg-white dark:bg-slate-950 flex flex-col items-center justify-center p-6">
        <div className="w-full max-w-lg relative">
          <div className="absolute -top-24 -left-24 w-64 h-64 bg-indigo-500/10 rounded-full blur-3xl"></div>
          <div className="absolute -bottom-24 -right-24 w-64 h-64 bg-rose-500/10 rounded-full blur-3xl"></div>

          <div className="text-center mb-12 relative">
            <div className="inline-flex items-center justify-center w-24 h-24 bg-gradient-to-br from-indigo-600 to-violet-700 rounded-[2.5rem] shadow-2xl shadow-indigo-500/40 mb-8 rotate-3">
              <Calendar className="w-12 h-12 text-white" />
            </div>
            <h1 className="text-5xl font-black text-slate-900 dark:text-white mb-4 tracking-tighter">
              EventConnect<span className="text-indigo-600">.</span>
            </h1>
            <p className="text-slate-500 dark:text-slate-400 font-semibold text-lg">Најдобрите настани, на дланка.</p>
          </div>

          <GlassCard className="p-10 border-t-8 border-t-indigo-600">
            <div className="space-y-6">
              <div>
                <label className="block text-xs font-black text-slate-400 uppercase tracking-[0.2em] mb-3 ml-1">Емаил адреса</label>
                <input
                  type="email"
                  className="w-full bg-slate-50 dark:bg-slate-800/50 border-2 border-slate-100 dark:border-slate-800 rounded-2xl py-4 px-5 focus:border-indigo-500 focus:outline-none transition-all dark:text-white font-medium"
                  placeholder="name@example.com"
                  value={loginForm.email}
                  onChange={(e) => setLoginForm({ ...loginForm, email: e.target.value })}
                />
              </div>
              <div>
                <label className="block text-xs font-black text-slate-400 uppercase tracking-[0.2em] mb-3 ml-1">Лозинка</label>
                <input
                  type="password"
                  className="w-full bg-slate-50 dark:bg-slate-800/50 border-2 border-slate-100 dark:border-slate-800 rounded-2xl py-4 px-5 focus:border-indigo-500 focus:outline-none transition-all dark:text-white font-medium"
                  placeholder="••••••••"
                  value={loginForm.password}
                  onChange={(e) => setLoginForm({ ...loginForm, password: e.target.value })}
                />
              </div>
              <button
                onClick={handleLogin}
                disabled={loading}
                className="w-full bg-slate-900 dark:bg-white text-white dark:text-slate-900 py-5 rounded-[1.5rem] font-black text-sm uppercase tracking-widest shadow-2xl hover:translate-y-[-2px] active:translate-y-0 transition-all disabled:opacity-50"
              >
                {loading ? 'Се најавува...' : 'Влези во светот на настаните'}
              </button>
            </div>
          </GlassCard>

          <div className="mt-8 text-center bg-indigo-50/50 dark:bg-slate-900/50 backdrop-blur-md p-6 rounded-[2rem] border border-indigo-100/50 dark:border-slate-800/50">
            <p className="text-[10px] text-indigo-600 dark:text-indigo-400 font-black uppercase tracking-[0.25em] mb-3">ТЕСТ ПОДАТОЦИ</p>
            <div className="space-y-2">
              <p className="text-xs font-bold text-slate-600 dark:text-slate-400">
                martin.stamenov03@gmail.com <span className="text-slate-400 font-medium mx-1">/</span> <span className="text-slate-900 dark:text-slate-200">test123</span>
              </p>
              <p className="text-xs font-bold text-slate-600 dark:text-slate-400">
                teodorasaneva@gmail.com <span className="text-slate-400 font-medium mx-1">/</span> <span className="text-slate-900 dark:text-slate-200">test123</span>
              </p>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-[#FDFDFF] dark:bg-slate-950 text-slate-900 dark:text-slate-100">
      {/* Header */}
      <header className="sticky top-0 z-50 bg-white/70 dark:bg-slate-950/70 backdrop-blur-2xl border-b border-slate-200/50 dark:border-slate-800/50">
        <div className="max-w-7xl mx-auto px-6 lg:px-10">
          <div className="flex justify-between items-center h-24">
            <div className="flex items-center gap-4 group cursor-pointer">
              <div className="w-12 h-12 bg-gradient-to-tr from-indigo-600 to-violet-600 rounded-2xl flex items-center justify-center shadow-lg shadow-indigo-500/30 group-hover:rotate-6 transition-transform">
                <Calendar className="w-7 h-7 text-white" />
              </div>
              <span className="text-2xl font-black tracking-tighter hidden sm:block">
                EventConnect<span className="text-indigo-600">.</span>
              </span>
            </div>

            <div className="flex items-center gap-4">
              <Notifications accessToken={accessToken} />
              <button
                onClick={() => setDarkMode(!darkMode)}
                className="p-3 rounded-2xl bg-slate-100 dark:bg-slate-900 text-slate-600 dark:text-slate-400 hover:bg-slate-200 dark:hover:bg-slate-800 transition-all"
              >
                {darkMode ? '☀️' : '🌙'}
              </button>

              <div className="flex items-center gap-4 pl-4 border-l border-slate-200 dark:border-slate-800">
                <div className="text-right hidden md:block">
                  <p className="text-sm font-black leading-none">{user?.name}</p>
                  <p className="text-[10px] font-black text-indigo-500 uppercase tracking-widest mt-1">PRO MEMBER</p>
                </div>
                <div className="w-12 h-12 rounded-2xl bg-indigo-50 dark:bg-slate-900 border-2 border-white dark:border-slate-800 flex items-center justify-center shadow-sm">
                  <User className="w-6 h-6 text-indigo-600" />
                </div>
                <button
                  onClick={() => {
                    setAccessToken(null);
                    setStoredToken(null);
                  }}
                  className="p-3 text-slate-400 hover:text-rose-500 transition-colors"
                >
                  <LogOut className="w-6 h-6" />
                </button>
              </div>
            </div>
          </div>
        </div>
      </header>

      {/* Navigation */}
      <nav className="max-w-7xl mx-auto px-6 py-8">
        <div className="flex items-center gap-3 overflow-x-auto scrollbar-hide pb-2">
          {[
            { id: 'discover', label: 'Истражи', icon: LayoutGrid },
            { id: 'calendar', label: 'Календар', icon: Calendar },
            { id: 'recommendations', label: 'За Тебе', icon: Sparkles },
            { id: 'group', label: 'Заеднички', icon: Users },
            { id: 'favorites', label: 'Омилени', icon: Heart },
            { id: 'friends', label: 'Пријатели', icon: UserPlus },
            { id: 'feed', label: 'Фид', icon: Activity },
          ].map(tab => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`flex items-center gap-2.5 px-6 py-3.5 rounded-2xl text-sm font-black whitespace-nowrap transition-all border-2 ${
                activeTab === tab.id
                  ? 'bg-indigo-600 border-indigo-600 text-white shadow-xl shadow-indigo-500/20 scale-105'
                  : 'bg-white dark:bg-slate-900 border-slate-200/50 dark:border-slate-800/50 text-slate-500 hover:border-indigo-300'
              }`}
            >
              <tab.icon className="w-4 h-4" />
              {tab.label}
            </button>
          ))}
        </div>
      </nav>

      {/* Content */}
      <main className="max-w-7xl mx-auto px-6 lg:px-10 pb-20">
        {activeTab === 'discover' && (
          <div className="space-y-8">
            <SearchComponent onSearch={(q, f) => console.log('Search:', q, f)} />
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-8">
              {events.map(event => (
                <EventCard
                  key={event.id}
                  event={event}
                  venue={venues[event.venue_id!]}
                  onToggleFavorite={handleToggleFavorite}
                  onToggleAttended={handleToggleAttended}
                />
              ))}
            </div>
          </div>
        )}

        {activeTab === 'calendar' && <CalendarView events={events} venues={venues} />}

        {activeTab === 'recommendations' && (
          <div>
            <div className="mb-12">
              <h2 className="text-4xl font-black text-slate-900 dark:text-white flex items-center gap-4">
                <Sparkles className="w-10 h-10 text-indigo-600" />
                Селектирано само за тебе
              </h2>
              <p className="text-slate-500 font-bold mt-2 ml-1">Врз база на твоите интереси</p>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-10">
              {recommendations.map(event => (
                <EventCard
                  key={event.id || event.event_id}
                  event={event}
                  venue={venues[event.venue_id!]}
                  showRecommendation
                  onToggleFavorite={handleToggleFavorite}
                  onToggleAttended={handleToggleAttended}
                />
              ))}
            </div>
          </div>
        )}

        {activeTab === 'group' && (
          <GroupRecommendations
            friends={friends}
            onGetRecommendations={handleGroupRecommendations}
            recommendations={groupRecs}
            venues={venues}
          />
        )}

        {activeTab === 'favorites' && (
          <div>
            <h2 className="text-3xl font-black mb-10 text-slate-900 dark:text-white flex items-center gap-4">
              <Heart className="w-10 h-10 text-rose-500 fill-current" />
              Твојата листа на желби ({favorites.length})
            </h2>
            {favorites.length > 0 ? (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
                {favorites.map(event => (
                  <EventCard
                    key={event.id}
                    event={event}
                    venue={venues[event.venue_id!]}
                    onToggleFavorite={handleToggleFavorite}
                    onToggleAttended={handleToggleAttended}
                  />
                ))}
              </div>
            ) : (
              <div className="text-center py-32 bg-slate-50 dark:bg-slate-900/50 rounded-[3rem] border-2 border-dashed border-slate-200 dark:border-slate-800">
                <Heart className="w-20 h-20 text-slate-200 dark:text-slate-800 mx-auto mb-6" />
                <p className="text-slate-400 font-black uppercase tracking-widest">Сеуште немаш омилени настани</p>
              </div>
            )}
          </div>
        )}

        {activeTab === 'friends' && (
          <div>
            <div className="flex justify-between items-center mb-12">
              <h2 className="text-3xl font-black">Твоите Пријатели ({friends.length})</h2>
              <button
                onClick={() => setShowFindFriends(true)}
                className="bg-indigo-600 text-white px-8 py-4 rounded-2xl font-black text-sm uppercase tracking-widest shadow-xl shadow-indigo-500/30 hover:scale-105 transition-transform">
                Најди пријатели
              </button>
            </div>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-8">
              {friends.map(friend => (
                <GlassCard key={friend.id} className="p-8 text-center">
                  <div className="w-24 h-24 bg-gradient-to-br from-indigo-50 to-indigo-100 dark:from-slate-800 dark:to-slate-900 rounded-[2rem] flex items-center justify-center mx-auto mb-6 shadow-inner border-2 border-white dark:border-slate-700">
                    <User className="w-12 h-12 text-indigo-600" />
                  </div>
                  <h3 className="text-xl font-bold text-slate-900 dark:text-white mb-1">{friend.name}</h3>
                  <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-8">{friend.city || 'Скопје'}</p>
                  <button className="w-full py-4 bg-slate-900 dark:bg-white text-white dark:text-slate-900 rounded-2xl text-xs font-black uppercase tracking-widest hover:opacity-90 transition-all">
                    Порака
                  </button>
                </GlassCard>
              ))}
            </div>
          </div>
        )}

        {activeTab === 'feed' && (
          <div>
            <h2 className="text-3xl font-black mb-10 text-slate-900 dark:text-white flex items-center gap-4">
              <Activity className="w-10 h-10 text-indigo-600" />
              Активност на пријатели
            </h2>
            {feed.length > 0 ? (
              <div className="space-y-4">
                {feed.map((item, idx) => (
                  <GlassCard key={idx} className="p-6">
                    <div className="flex items-center gap-4">
                      <div className="w-12 h-12 bg-indigo-100 dark:bg-slate-800 rounded-2xl flex items-center justify-center">
                        <User className="w-6 h-6 text-indigo-600" />
                      </div>
                      <div className="flex-1">
                        <p className="text-slate-900 dark:text-white font-bold">
                          <span className="text-indigo-600">{item.user?.name || 'Корисник'}</span>
                          {' '}
                          {item.action === 'liked' ? 'лајкнал' : item.action === 'attended' ? 'присуствувал на' : 'дислајкнал'}
                          {' '}
                          <span className="font-black">{item.event?.title || 'настан'}</span>
                        </p>
                        <p className="text-xs text-slate-400 mt-1">{new Date(item.timestamp).toLocaleDateString('mk-MK')}</p>
                      </div>
                    </div>
                  </GlassCard>
                ))}
              </div>
            ) : (
              <div className="text-center py-32 bg-slate-50 dark:bg-slate-900/50 rounded-[3rem] border-2 border-dashed border-slate-200 dark:border-slate-800">
                <Activity className="w-20 h-20 text-slate-200 dark:text-slate-800 mx-auto mb-6" />
                <p className="text-slate-400 font-black uppercase tracking-widest">Нема активност од пријатели</p>
              </div>
            )}
          </div>
        )}
      </main>

      {showFindFriends && (
        <div className="fixed inset-0 bg-black/50 backdrop-blur-sm z-50 flex items-center justify-center p-6">
          <GlassCard className="w-full max-w-2xl p-8 relative">
            <button
              onClick={() => {
                setShowFindFriends(false);
                setSearchQuery('');
                setSearchResults([]);
              }}
              className="absolute top-6 right-6 text-slate-400 hover:text-slate-600 transition">
              <X className="w-6 h-6" />
            </button>

            <h2 className="text-3xl font-black text-slate-900 dark:text-white mb-6">Најди Пријатели</h2>

            <div className="flex gap-3 mb-6">
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                onKeyPress={(e) => e.key === 'Enter' && handleSearchUsers()}
                placeholder="Пребарај по име или email..."
                className="flex-1 px-4 py-3 border-2 border-slate-200 dark:border-slate-700 rounded-xl bg-white dark:bg-slate-900 text-slate-900 dark:text-white focus:border-indigo-500 focus:outline-none"
              />
              <button
                onClick={handleSearchUsers}
                disabled={loading}
                className="px-6 py-3 bg-indigo-600 text-white rounded-xl font-black hover:bg-indigo-700 transition">
                Барај
              </button>
            </div>

            <div className="max-h-96 overflow-y-auto space-y-3">
              {searchResults.length > 0 ? (
                searchResults.map((user) => (
                  <div key={user.id} className="flex items-center justify-between p-4 bg-slate-50 dark:bg-slate-800 rounded-xl">
                    <div className="flex items-center gap-4">
                      <div className="w-12 h-12 bg-indigo-100 dark:bg-slate-700 rounded-xl flex items-center justify-center">
                        <User className="w-6 h-6 text-indigo-600" />
                      </div>
                      <div>
                        <p className="font-bold text-slate-900 dark:text-white">{user.name}</p>
                        <p className="text-xs text-slate-500">{user.email} • {user.city || 'N/A'}</p>
                      </div>
                    </div>
                    {user.friendship_status === 'none' && (
                      <button
                        onClick={() => handleFriendRequest(user.id)}
                        className="px-4 py-2 bg-indigo-600 text-white rounded-lg text-sm font-black hover:bg-indigo-700 transition">
                        Додади
                      </button>
                    )}
                    {user.friendship_status === 'requested' && (
                      <span className="px-4 py-2 bg-slate-200 dark:bg-slate-700 text-slate-600 dark:text-slate-400 rounded-lg text-sm font-black">
                        Испратено
                      </span>
                    )}
                    {user.friendship_status === 'friends' && (
                      <span className="px-4 py-2 bg-emerald-100 dark:bg-emerald-900/20 text-emerald-600 dark:text-emerald-400 rounded-lg text-sm font-black">
                        Пријател
                      </span>
                    )}
                  </div>
                ))
              ) : searchQuery ? (
                <div className="text-center py-12 text-slate-400">
                  Нема резултати
                </div>
              ) : (
                <div className="text-center py-12 text-slate-400">
                  Внеси име или email за пребарување
                </div>
              )}
            </div>
          </GlassCard>
        </div>
      )}

      <footer className="max-w-7xl mx-auto px-6 py-16 border-t border-slate-200 dark:border-slate-800 text-center">
        <div className="flex justify-center items-center gap-3 mb-6">
          <div className="w-8 h-8 bg-slate-200 dark:bg-slate-800 rounded-lg"></div>
          <span className="text-sm font-black tracking-widest text-slate-400 uppercase">EventConnect v2.5</span>
        </div>
        <p className="text-slate-400 text-xs font-bold uppercase tracking-widest">&copy; 2024 • Теодора Санева & Мартин Стаменов</p>
      </footer>
    </div>
  );
};

export default EventSocialNetwork;
