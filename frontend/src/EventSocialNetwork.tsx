import { useState, useEffect, useMemo } from 'react';
import { Calendar, MapPin, Users, Heart, Star, Search, Bell, User, Clock, Tag, ChevronLeft, ChevronRight, UserPlus, CheckCircle, MessageCircle, Settings, LogOut, ThumbsDown, X } from 'lucide-react';

const BACKEND_BASE = "http://127.0.0.1:5000";

// Типови и интерфејси
interface Event {
  id?: number;
  event_id?: number;
  title: string;
  description?: string;
  starts_at: string;
  venue_id?: number;
  tags?: string[];
  my_rating?: number;
  score_pct?: number;
  liked_at?: string;
  attended_at?: string;
}

interface Venue {
  id: number;
  name: string;
  city?: string;
}

interface User {
  id: number;
  name: string;
  email: string;
  city?: string;
}

interface Friend {
  id: number;
  name: string;
  city?: string;
  friendship_status?: string;
}

interface DateInfo {
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

interface Filters {
  start: string;
  end: string;
  city: string;
  tags: string;
  search: string;
  sortBy?: string;
}

interface LoginForm {
  email: string;
  password: string;
}

interface VenuesMap {
  [key: number]: Venue;
}

interface EventsByDate {
  [key: number]: Event[];
}

// Local Storage helpers
const getStoredTheme = (): boolean => {
  try {
    const stored = window.localStorage.getItem('darkMode');
    return stored ? JSON.parse(stored) : false;
  } catch {
    return false;
  }
};

const setStoredTheme = (isDark: boolean): void => {
  try {
    window.localStorage.setItem('darkMode', JSON.stringify(isDark));
  } catch {
    // Ignore storage errors
  }
};

const getStoredToken = (): string | null => {
  try {
    return window.localStorage.getItem('access_token');
  } catch {
    return null;
  }
};

const setStoredToken = (token: string | null): void => {
  try {
    if (token) {
      window.localStorage.setItem('access_token', token);
    } else {
      window.localStorage.removeItem('access_token');
    }
  } catch {
    // Ignore storage errors
  }
};

// Helper функции за датуми
const formatEventDate = (dateStr: string): DateInfo => {
  try {
    const date = new Date(dateStr);
    const now = new Date();
    const diffTime = date.getTime() - now.getTime();
    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));

    const dayNames = ['Недела', 'Понеделник', 'Вторник', 'Среда', 'Четврток', 'Петок', 'Сабота'];
    const monthNames = ['јануари', 'февруари', 'март', 'април', 'мај', 'јуни', 'јули', 'август', 'септември', 'октомври', 'ноември', 'декември'];

    const dayName = dayNames[date.getDay()];
    const day = date.getDate();
    const month = monthNames[date.getMonth()];
    const year = date.getFullYear();
    const time = date.toLocaleTimeString('mk-MK', { hour: '2-digit', minute: '2-digit' });

    let relativeDay = '';
    if (diffDays === 0) relativeDay = 'Денес';
    else if (diffDays === 1) relativeDay = 'Утре';
    else if (diffDays === -1) relativeDay = 'Вчера';
    else if (diffDays > 1 && diffDays <= 7) relativeDay = `За ${diffDays} дена`;
    else if (diffDays < -1 && diffDays >= -7) relativeDay = `Пред ${Math.abs(diffDays)} дена`;

    return {
      full: `${dayName}, ${day} ${month} ${year}`,
      day: day,
      month: month.substring(0, 3),
      time: time,
      relative: relativeDay,
      dayName: dayName.substring(0, 3),
      isPast: diffDays < 0,
      isToday: diffDays === 0,
      isTomorrow: diffDays === 1
    };
  } catch (error) {
    console.error('Error formatting date:', error);
    return {
      full: 'Невалиден датум',
      day: 0,
      month: '?',
      time: '?',
      relative: '',
      dayName: '?',
      isPast: false,
      isToday: false,
      isTomorrow: false
    };
  }
};

// Event Card компонента
interface EventCardProps {
  event: Event;
  venue?: Venue;
  onToggleFavorite?: (eventId: number) => Promise<void>;
  onToggleAttended?: (eventId: number) => Promise<void>;
  onDislike?: (eventId: number) => Promise<void>;
  showRecommendation?: boolean;
  showActions?: boolean;
}

const EventCard: React.FC<EventCardProps> = ({
  event,
  venue,
  onToggleFavorite,
  onToggleAttended,
  onDislike,
  showRecommendation = false,
  showActions = true
}) => {
  const [isLiked, setIsLiked] = useState<boolean>(false);
  const [isDisliked, setIsDisliked] = useState<boolean>(false);
  const [hasAttended, setHasAttended] = useState<boolean>(false);
  const [loading, setLoading] = useState<boolean>(false);

  useEffect(() => {
    if (event && typeof event.my_rating !== 'undefined') {
      setIsLiked(event.my_rating === 1);
      setIsDisliked(event.my_rating === -1);
      setHasAttended(!!event.my_rating);
    }
  }, [event]);

  const dateInfo = formatEventDate(event?.starts_at || new Date().toISOString());
  const tags = Array.isArray(event?.tags) ? event.tags.filter((t: string) => t && t.trim()) : [];

  const handleToggleFavorite = async (): Promise<void> => {
    if (!onToggleFavorite || !event) return;

    setLoading(true);
    try {
      const eventId = event.id || event.event_id;
      if (eventId) {
        await onToggleFavorite(eventId);
        setIsLiked(!isLiked);
      }
    } catch (error) {
      console.error('Error toggling favorite:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleToggleAttended = async (): Promise<void> => {
    if (!onToggleAttended || !event) return;

    setLoading(true);
    try {
      const eventId = event.id || event.event_id;
      if (eventId) {
        await onToggleAttended(eventId);
        setHasAttended(!hasAttended);
      }
    } catch (error) {
      console.error('Error toggling attended:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleDislike = async (): Promise<void> => {
    if (!onDislike || !event) return;

    setLoading(true);
    try {
      const eventId = event.id || event.event_id;
      if (eventId) {
        await onDislike(eventId);
        setIsDisliked(!isDisliked);
        if (!isDisliked) {
          setIsLiked(false); // Clear like when disliking
        }
      }
    } catch (error) {
      console.error('Error disliking event:', error);
    } finally {
      setLoading(false);
    }
  };

  if (!event) return null;

  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl shadow-lg hover:shadow-xl transition-all duration-300 overflow-hidden border border-gray-100 dark:border-gray-700">
      {/* Header со date badge */}
      <div className="relative p-6 pb-4">
        <div className={`absolute top-4 right-4 rounded-lg px-3 py-2 text-sm font-medium ${
          dateInfo.isPast ? 'bg-gray-400' : 
          dateInfo.isToday ? 'bg-green-500' : 
          dateInfo.isTomorrow ? 'bg-orange-500' : 'bg-indigo-500'
        } text-white`}>
          <div className="text-center">
            <div className="text-lg font-bold">{dateInfo.day}</div>
            <div className="text-xs uppercase">{dateInfo.month}</div>
          </div>
        </div>

        <h3 className="text-xl font-bold text-gray-900 dark:text-white mb-2 pr-20">
          {event.title || 'Без наслов'}
        </h3>

        {/* Датум и време подетално */}
        <div className="mb-3">
          <div className="flex items-center text-gray-600 dark:text-gray-400 mb-1">
            <Calendar className="w-4 h-4 mr-2" />
            <span className="text-sm font-medium">{dateInfo.full}</span>
          </div>
          <div className="flex items-center text-gray-500 dark:text-gray-500 text-sm">
            <Clock className="w-3 h-3 mr-2" />
            <span>{dateInfo.time}</span>
            {dateInfo.relative && (
              <>
                <span className="mx-2">•</span>
                <span className={`font-medium ${
                  dateInfo.isToday ? 'text-green-600' : 
                  dateInfo.isTomorrow ? 'text-orange-600' : 
                  dateInfo.isPast ? 'text-gray-500' : 'text-indigo-600'
                }`}>
                  {dateInfo.relative}
                </span>
              </>
            )}
          </div>
        </div>

        {showRecommendation && event.score_pct && (
          <div className="mb-3">
            <div className="flex items-center justify-between text-sm text-gray-600 dark:text-gray-400 mb-1">
              <span>Препорака за тебе</span>
              <span className="font-semibold text-indigo-600">{event.score_pct}%</span>
            </div>
            <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2">
              <div
                className="h-2 rounded-full bg-gradient-to-r from-indigo-500 to-purple-600 transition-all duration-500"
                style={{ width: `${event.score_pct}%` }}
              ></div>
            </div>
          </div>
        )}
      </div>

      {/* Event details */}
      <div className="px-6 pb-4">
        {venue && (
          <div className="flex items-center text-gray-600 dark:text-gray-400 mb-3">
            <MapPin className="w-4 h-4 mr-2" />
            <span className="text-sm">{venue.name}</span>
            {venue.city && <span className="text-sm ml-1 text-gray-500">• {venue.city}</span>}
          </div>
        )}

        {event.description && (
          <p className="text-gray-700 dark:text-gray-300 text-sm mb-4 line-clamp-3">
            {event.description.length > 120 ?
              event.description.substring(0, 120) + '...' :
              event.description
            }
          </p>
        )}

        {/* Tags */}
        {tags.length > 0 && (
          <div className="flex flex-wrap gap-2 mb-4">
            {tags.slice(0, 3).map((tag: string, i: number) => (
              <span key={i} className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-indigo-100 text-indigo-800 dark:bg-indigo-900 dark:text-indigo-200">
                <Tag className="w-3 h-3 mr-1" />
                {tag}
              </span>
            ))}
            {tags.length > 3 && (
              <span className="text-xs text-gray-500">+{tags.length - 3} повеќе</span>
            )}
          </div>
        )}
      </div>

      {/* Action buttons */}
      {showActions && (
        <div className="px-6 py-4 bg-gray-50 dark:bg-gray-900 border-t border-gray-100 dark:border-gray-700">
          <div className="flex items-center justify-between">
            <div className="flex space-x-2">
              <button
                onClick={handleToggleFavorite}
                disabled={loading}
                className={`flex items-center px-3 py-2 rounded-lg text-sm font-medium transition-all ${
                  isLiked
                    ? 'bg-red-100 text-red-600 dark:bg-red-900 dark:text-red-300'
                    : 'bg-gray-100 text-gray-600 hover:bg-red-50 hover:text-red-600 dark:bg-gray-700 dark:text-gray-400'
                } ${loading ? 'opacity-50 cursor-not-allowed' : ''}`}
              >
                <Heart className={`w-4 h-4 mr-1 ${isLiked ? 'fill-current' : ''}`} />
                Омилен
              </button>

              <button
                onClick={handleDislike}
                disabled={loading}
                className={`flex items-center px-3 py-2 rounded-lg text-sm font-medium transition-all ${
                  isDisliked
                    ? 'bg-red-100 text-red-600 dark:bg-red-900 dark:text-red-300'
                    : 'bg-gray-100 text-gray-600 hover:bg-red-50 hover:text-red-600 dark:bg-gray-700 dark:text-gray-400'
                } ${loading ? 'opacity-50 cursor-not-allowed' : ''}`}
              >
                <ThumbsDown className={`w-4 h-4 mr-1 ${isDisliked ? 'fill-current' : ''}`} />
                Не ми се допаѓа
              </button>

              <button
                onClick={handleToggleAttended}
                disabled={loading}
                className={`flex items-center px-3 py-2 rounded-lg text-sm font-medium transition-all ${
                  hasAttended
                    ? 'bg-green-100 text-green-600 dark:bg-green-900 dark:text-green-300'
                    : 'bg-gray-100 text-gray-600 hover:bg-green-50 hover:text-green-600 dark:bg-gray-700 dark:text-gray-400'
                } ${loading ? 'opacity-50 cursor-not-allowed' : ''}`}
              >
                <CheckCircle className={`w-4 h-4 mr-1 ${hasAttended ? 'fill-current' : ''}`} />
                Присуствувал
              </button>
            </div>

            <button className="flex items-center px-3 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 text-sm font-medium transition-all">
              <Users className="w-4 h-4 mr-1" />
              Детали
            </button>
          </div>
        </div>
      )}
    </div>
  );
};

// Calendar View компонента
interface CalendarViewProps {
  events: Event[];
  venues: VenuesMap;
}

const CalendarView: React.FC<CalendarViewProps> = ({ events, venues }) => {
  const [currentDate, setCurrentDate] = useState<Date>(new Date());
  const [selectedEvent, setSelectedEvent] = useState<Event | null>(null);

  const daysInMonth = new Date(currentDate.getFullYear(), currentDate.getMonth() + 1, 0).getDate();
  const firstDay = new Date(currentDate.getFullYear(), currentDate.getMonth(), 1).getDay();
  const startingDayOfWeek = firstDay === 0 ? 6 : firstDay - 1;

  const eventsByDate = useMemo((): EventsByDate => {
    const grouped: EventsByDate = {};
    if (Array.isArray(events)) {
      events.forEach((event: Event) => {
        if (event.starts_at) {
          const eventDate = new Date(event.starts_at);
          if (eventDate.getMonth() === currentDate.getMonth() &&
              eventDate.getFullYear() === currentDate.getFullYear()) {
            const date = eventDate.getDate();
            if (!grouped[date]) grouped[date] = [];
            grouped[date].push(event);
          }
        }
      });
    }
    return grouped;
  }, [events, currentDate]);

  const monthNames = ['Јануари', 'Февруари', 'Март', 'Април', 'Мај', 'Јуни', 'Јули', 'Август', 'Септември', 'Октомври', 'Ноември', 'Декември'];
  const dayNames = ['Пон', 'Вто', 'Сре', 'Чет', 'Пет', 'Саб', 'Нед'];

  const navigateMonth = (direction: number): void => {
    const newDate = new Date(currentDate);
    newDate.setMonth(currentDate.getMonth() + direction);
    setCurrentDate(newDate);
  };

  const goToToday = (): void => {
    setCurrentDate(new Date());
  };

  const getDayEvents = (day: number): Event[] => eventsByDate[day] || [];

  return (
    <div className="space-y-6">
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-6">
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-2xl font-bold text-gray-900 dark:text-white">
            {monthNames[currentDate.getMonth()]} {currentDate.getFullYear()}
          </h2>
          <div className="flex items-center space-x-3">
            <button
              onClick={goToToday}
              className="px-4 py-2 text-sm font-medium text-indigo-600 bg-indigo-50 rounded-lg hover:bg-indigo-100 transition-colors"
            >
              Денес
            </button>
            <button
              onClick={() => navigateMonth(-1)}
              className="p-2 text-gray-600 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg transition-colors"
            >
              <ChevronLeft className="w-5 h-5" />
            </button>
            <button
              onClick={() => navigateMonth(1)}
              className="p-2 text-gray-600 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg transition-colors"
            >
              <ChevronRight className="w-5 h-5" />
            </button>
          </div>
        </div>

        <div className="grid grid-cols-7 gap-1">
          {dayNames.map((day: string) => (
            <div key={day} className="p-3 text-center text-sm font-medium text-gray-500 dark:text-gray-400 bg-gray-50 dark:bg-gray-700 rounded-lg">
              {day}
            </div>
          ))}

          {Array.from({ length: startingDayOfWeek }, (_, i: number) => (
            <div key={`empty-${i}`} className="p-1 h-24"></div>
          ))}

          {Array.from({ length: daysInMonth }, (_, i: number) => {
            const day = i + 1;
            const dayEvents = getDayEvents(day);
            const isToday = new Date().toDateString() === new Date(currentDate.getFullYear(), currentDate.getMonth(), day).toDateString();

            return (
              <div key={day} className="p-1 h-24">
                <div className={`h-full p-2 rounded-lg border transition-all cursor-pointer ${
                  dayEvents.length > 0 
                    ? 'bg-indigo-50 border-indigo-200 hover:bg-indigo-100 dark:bg-indigo-900 dark:border-indigo-700' 
                    : 'border-gray-200 hover:bg-gray-50 dark:border-gray-700 dark:hover:bg-gray-800'
                } ${isToday ? 'ring-2 ring-indigo-500' : ''}`}>
                  <div className={`text-sm font-medium mb-1 ${
                    isToday ? 'text-indigo-600' : 'text-gray-700 dark:text-gray-300'
                  }`}>
                    {day}
                  </div>

                  <div className="space-y-1">
                    {dayEvents.slice(0, 2).map((event: Event, idx: number) => (
                      <div
                        key={`${event.id || event.event_id || idx}-${idx}`}
                        onClick={() => setSelectedEvent(event)}
                        className="text-xs bg-indigo-200 text-indigo-800 px-2 py-1 rounded truncate hover:bg-indigo-300 transition-colors"
                        title={event.title || 'Настан'}
                      >
                        {(event.title || 'Настан').length > 15 ? (event.title || 'Настан').substring(0, 15) + '...' : (event.title || 'Настан')}
                      </div>
                    ))}
                    {dayEvents.length > 2 && (
                      <div className="text-xs text-indigo-600 font-medium">
                        +{dayEvents.length - 2} повеќе
                      </div>
                    )}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {selectedEvent && selectedEvent.starts_at && (
        <div className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-6">
          <div className="flex items-start justify-between mb-4">
            <h3 className="text-xl font-bold text-gray-900 dark:text-white">
              Настани за {formatEventDate(selectedEvent.starts_at).full}
            </h3>
            <button
              onClick={() => setSelectedEvent(null)}
              className="text-gray-400 hover:text-gray-600 transition-colors text-lg"
            >
              ✕
            </button>
          </div>

          <div className="space-y-4">
            {getDayEvents(new Date(selectedEvent.starts_at).getDate()).map((event: Event) => (
              <div key={event.id || event.event_id || Math.random()} className="border-l-4 border-indigo-500 pl-4 py-2">
                <h4 className="font-semibold text-gray-900 dark:text-white">{event.title || 'Настан'}</h4>
                <div className="flex items-center text-sm text-gray-600 dark:text-gray-400 mt-1">
                  <Clock className="w-4 h-4 mr-2" />
                  <span>{formatEventDate(event.starts_at || '').time}</span>
                  {venues && event.venue_id && venues[event.venue_id] && (
                    <>
                      <span className="mx-2">•</span>
                      <MapPin className="w-4 h-4 mr-1" />
                      <span>{venues[event.venue_id].name}</span>
                    </>
                  )}
                </div>
                {event.description && (
                  <p className="text-sm text-gray-700 dark:text-gray-300 mt-2">
                    {event.description.length > 100 ?
                      event.description.substring(0, 100) + '...' :
                      event.description
                    }
                  </p>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

// Search component
interface SearchComponentProps {
  onSearch: (query: string, filters: Partial<Filters>) => void;
  loading?: boolean;
}

const SearchComponent: React.FC<SearchComponentProps> = ({ onSearch, loading = false }) => {
  const [query, setQuery] = useState<string>('');
  const [filters, setFilters] = useState<Partial<Filters>>({});
  const [showAdvanced, setShowAdvanced] = useState<boolean>(false);
  const [selectedTags, setSelectedTags] = useState<string[]>([]);

  // Common event tags
  const availableTags = [
    'IT', 'Music', 'Theater', 'Sports', 'Art', 'Food',
    'Business', 'Education', 'Science', 'Entertainment',
    'Health', 'Fashion', 'Technology', 'Culture'
  ];

  const handleSearch = () => {
    const searchFilters = {
      ...filters,
      tags: selectedTags.length > 0 ? selectedTags.join(',') : ''
    };
    onSearch(query, searchFilters);
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
  };

  const toggleTag = (tag: string) => {
    setSelectedTags(prev =>
      prev.includes(tag)
        ? prev.filter(t => t !== tag)
        : [...prev, tag]
    );
  };

  const clearFilters = () => {
    setFilters({});
    setSelectedTags([]);
    setQuery('');
    onSearch('', {});
  };

  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-6 mb-6">
      <div className="flex gap-4 mb-4">
        <div className="flex-1 relative">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-5 h-5" />
          <input
            type="text"
            placeholder="Пребарај настани..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyPress={handleKeyPress}
            className="w-full pl-10 pr-4 py-3 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white focus:ring-2 focus:ring-indigo-500 focus:border-transparent"
          />
        </div>
        <button
          onClick={handleSearch}
          disabled={loading}
          className={`px-6 py-3 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 font-medium transition-colors ${
            loading ? 'opacity-50 cursor-not-allowed' : ''
          }`}
        >
          {loading ? 'Пребарувам...' : 'Пребарај'}
        </button>
        <button
          onClick={() => setShowAdvanced(!showAdvanced)}
          className={`px-4 py-3 border rounded-lg transition-colors ${
            showAdvanced
              ? 'bg-indigo-100 border-indigo-500 dark:bg-indigo-900 dark:border-indigo-500'
              : 'border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700'
          }`}
        >
          <Settings className="w-5 h-5" />
        </button>
      </div>

      {showAdvanced && (
        <div className="space-y-4 pt-4 border-t border-gray-200 dark:border-gray-700">
          {/* Tags/Categories */}
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Категории / Тагови
            </label>
            <div className="flex flex-wrap gap-2">
              {availableTags.map(tag => (
                <button
                  key={tag}
                  onClick={() => toggleTag(tag)}
                  className={`px-3 py-1.5 rounded-full text-sm font-medium transition-all ${
                    selectedTags.includes(tag)
                      ? 'bg-indigo-600 text-white'
                      : 'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'
                  }`}
                >
                  {tag}
                </button>
              ))}
            </div>
            {selectedTags.length > 0 && (
              <div className="mt-2 text-sm text-gray-600 dark:text-gray-400">
                Избрани: {selectedTags.join(', ')}
              </div>
            )}
          </div>

          {/* Date Range and City */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                Од датум
              </label>
              <input
                type="date"
                value={filters.start || ''}
                onChange={(e) => setFilters({...filters, start: e.target.value})}
                className="w-full p-3 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white focus:ring-2 focus:ring-indigo-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                До датум
              </label>
              <input
                type="date"
                value={filters.end || ''}
                onChange={(e) => setFilters({...filters, end: e.target.value})}
                className="w-full p-3 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white focus:ring-2 focus:ring-indigo-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                Град
              </label>
              <input
                type="text"
                placeholder="Внесете град..."
                value={filters.city || ''}
                onChange={(e) => setFilters({...filters, city: e.target.value})}
                className="w-full p-3 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white focus:ring-2 focus:ring-indigo-500"
              />
            </div>
          </div>

          {/* Sort Options */}
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Сортирај по
            </label>
            <select
              value={filters.sortBy || ''}
              onChange={(e) => setFilters({...filters, sortBy: e.target.value})}
              className="w-full p-3 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white focus:ring-2 focus:ring-indigo-500"
            >
              <option value="">Релевантност</option>
              <option value="date_newest">Датум (Најнови)</option>
              <option value="date_oldest">Датум (Најстари)</option>
              <option value="relevance">Релевантност</option>
            </select>
          </div>

          {/* Clear Filters Button */}
          <div className="flex justify-end">
            <button
              onClick={clearFilters}
              className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 bg-gray-100 dark:bg-gray-700 rounded-lg hover:bg-gray-200 dark:hover:bg-gray-600 transition-colors"
            >
              <X className="w-4 h-4" />
              Ресетирај филтри
            </button>
          </div>
        </div>
      )}
    </div>
  );
};

// Notification Badge Component
interface NotificationsProps {
  accessToken: string | null;
}

const Notifications: React.FC<NotificationsProps> = ({ accessToken }) => {
  const [notifications, setNotifications] = useState<any[]>([]);
  const [showDropdown, setShowDropdown] = useState<boolean>(false);
  const [unreadCount, setUnreadCount] = useState<number>(0);

  useEffect(() => {
    if (accessToken) {
      loadNotifications();
      // Poll for new notifications every 30 seconds
      const interval = setInterval(loadNotifications, 30000);
      return () => clearInterval(interval);
    }
  }, [accessToken]);

  const loadNotifications = async () => {
    try {
      const response = await fetch(`${BACKEND_BASE}/api/friends/pending`, {
        headers: {
          'Authorization': `Bearer ${accessToken}`
        }
      });

      if (response.ok) {
        const data = await response.json();
        setNotifications(data);
        setUnreadCount(data.length);
      }
    } catch (error) {
      console.error('Error loading notifications:', error);
    }
  };

  const acceptFriend = async (requesterId: number) => {
    try {
      await fetch(`${BACKEND_BASE}/api/friends/accept`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${accessToken}`
        },
        body: JSON.stringify({ user_id: requesterId })
      });

      loadNotifications(); // Reload notifications
    } catch (error) {
      console.error('Error accepting friend:', error);
    }
  };

  return (
    <div className="relative">
      <button
        onClick={() => setShowDropdown(!showDropdown)}
        className="p-2 text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg relative"
      >
        <Bell className="w-5 h-5" />
        {unreadCount > 0 && (
          <span className="absolute top-1 right-1 w-4 h-4 bg-red-500 text-white text-xs rounded-full flex items-center justify-center">
            {unreadCount}
          </span>
        )}
      </button>

      {showDropdown && (
        <div className="absolute right-0 mt-2 w-80 bg-white dark:bg-gray-800 rounded-lg shadow-xl border border-gray-200 dark:border-gray-700 z-50">
          <div className="p-4 border-b border-gray-200 dark:border-gray-700">
            <h3 className="font-semibold text-gray-900 dark:text-white">Нотификации</h3>
          </div>

          <div className="max-h-96 overflow-y-auto">
            {notifications.length > 0 ? (
              notifications.map((notif, index) => (
                <div key={index} className="p-4 border-b border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700">
                  <div className="flex items-start space-x-3">
                    <div className="w-10 h-10 bg-indigo-500 rounded-full flex items-center justify-center">
                      <UserPlus className="w-5 h-5 text-white" />
                    </div>
                    <div className="flex-1">
                      <p className="text-sm text-gray-900 dark:text-white">
                        <span className="font-semibold">{notif.requester_name}</span> сака да те додаде како пријател
                      </p>
                      <div className="flex space-x-2 mt-2">
                        <button
                          onClick={() => acceptFriend(notif.requester_id)}
                          className="px-3 py-1 bg-indigo-600 text-white rounded text-xs hover:bg-indigo-700"
                        >
                          Прифати
                        </button>
                        <button
                          onClick={() => setShowDropdown(false)}
                          className="px-3 py-1 bg-gray-200 text-gray-700 rounded text-xs hover:bg-gray-300"
                        >
                          Подоцна
                        </button>
                      </div>
                    </div>
                  </div>
                </div>
              ))
            ) : (
              <div className="p-8 text-center text-gray-500">
                Нема нови нотификации
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

// User Profile Modal Component
interface UserProfileProps {
  userId: number;
  onClose: () => void;
  accessToken: string | null;
}

const UserProfileModal: React.FC<UserProfileProps> = ({ userId, onClose, accessToken }) => {
  const [profile, setProfile] = useState<any>(null);
  const [loading, setLoading] = useState<boolean>(true);

  useEffect(() => {
    loadProfile();
  }, [userId]);

  const loadProfile = async () => {
    try {
      const response = await fetch(`${BACKEND_BASE}/api/user/profile/${userId}`, {
        headers: {
          'Authorization': `Bearer ${accessToken}`
        }
      });

      if (response.ok) {
        const data = await response.json();
        setProfile(data);
      }
    } catch (error) {
      console.error('Error loading profile:', error);
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
        <div className="bg-white dark:bg-gray-800 rounded-xl p-8">
          <div className="animate-pulse">Вчитувам профил...</div>
        </div>
      </div>
    );
  }

  if (!profile) {
    return null;
  }

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50" onClick={onClose}>
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow-2xl max-w-2xl w-full m-4" onClick={(e) => e.stopPropagation()}>
        <div className="p-6 border-b border-gray-200 dark:border-gray-700 flex justify-between items-center">
          <h2 className="text-2xl font-bold text-gray-900 dark:text-white">Профил</h2>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600">
            <span className="text-2xl">×</span>
          </button>
        </div>

        <div className="p-6">
          <div className="flex items-center mb-6">
            <div className="w-20 h-20 bg-gradient-to-r from-indigo-500 to-purple-600 rounded-full flex items-center justify-center mr-4">
              <User className="w-10 h-10 text-white" />
            </div>
            <div>
              <h3 className="text-2xl font-bold text-gray-900 dark:text-white">{profile.name}</h3>
              <p className="text-gray-600 dark:text-gray-400">{profile.email}</p>
              {profile.city && (
                <p className="text-sm text-gray-500 mt-1">📍 {profile.city}</p>
              )}
            </div>
          </div>

          <div className="grid grid-cols-2 gap-4 mb-6">
            <div className="bg-indigo-50 dark:bg-indigo-900 p-4 rounded-lg">
              <div className="text-2xl font-bold text-indigo-600 dark:text-indigo-300">
                {profile.stats.total_attended}
              </div>
              <div className="text-sm text-gray-600 dark:text-gray-400">Настани присуствувани</div>
            </div>
            <div className="bg-pink-50 dark:bg-pink-900 p-4 rounded-lg">
              <div className="text-2xl font-bold text-pink-600 dark:text-pink-300">
                {profile.stats.total_liked}
              </div>
              <div className="text-sm text-gray-600 dark:text-gray-400">Омилени настани</div>
            </div>
          </div>

          <div>
            <h4 className="font-semibold text-gray-900 dark:text-white mb-3">Последни активности</h4>
            <div className="space-y-2">
              {profile.recent_activities.slice(0, 5).map((activity: any, index: number) => (
                <div key={index} className="flex items-center justify-between p-3 bg-gray-50 dark:bg-gray-700 rounded-lg">
                  <div className="flex-1">
                    <p className="text-sm font-medium text-gray-900 dark:text-white">{activity.event_title}</p>
                    <p className="text-xs text-gray-500">
                      {activity.rating === 1 ? '❤️ Лајк' : activity.rating === -1 ? '👎 Дислајк' : '✓ Присуствувал'}
                    </p>
                  </div>
                  <span className="text-xs text-gray-400">
                    {new Date(activity.date).toLocaleDateString('mk-MK')}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

// Friends Search Component
interface FriendsSearchProps {
  onAddFriend: (userId: number) => Promise<void>;
  onViewProfile: (userId: number) => void;
}

const FriendsSearch: React.FC<FriendsSearchProps> = ({ onAddFriend, onViewProfile }) => {
  const [query, setQuery] = useState<string>('');
  const [results, setResults] = useState<Friend[]>([]);
  const [loading, setLoading] = useState<boolean>(false);

  const searchUsers = async () => {
    if (query.length < 2) {
      setResults([]);
      return;
    }

    setLoading(true);
    try {
      const token = getStoredToken();
      const response = await fetch(`${BACKEND_BASE}/api/user/search`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({ query })
      });

      if (response.ok) {
        const data = await response.json();
        setResults(data);
      }
    } catch (error) {
      console.error('Error searching users:', error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    const timer = setTimeout(searchUsers, 300);
    return () => clearTimeout(timer);
  }, [query]);

  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-6">
      <h3 className="text-lg font-bold text-gray-900 dark:text-white mb-4">
        Додај пријатели
      </h3>

      <div className="relative mb-4">
        <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-5 h-5" />
        <input
          type="text"
          placeholder="Пребарај корисници..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          className="w-full pl-10 pr-4 py-3 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
        />
      </div>

      {loading && (
        <div className="text-center py-4 text-gray-500">Пребарувам...</div>
      )}

      <div className="space-y-2">
        {results.map((user) => (
          <div key={user.id} className="flex items-center justify-between p-3 border border-gray-200 dark:border-gray-700 rounded-lg">
            <div>
              <div className="font-semibold text-gray-900 dark:text-white">{user.name}</div>
              {user.city && (
                <div className="text-sm text-gray-500">{user.city}</div>
              )}
            </div>

            <div className="flex items-center space-x-2">
              {user.friendship_status === 'none' ? (
                <button
                  onClick={() => onAddFriend(user.id)}
                  className="flex items-center px-3 py-1 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 text-sm"
                >
                  <UserPlus className="w-4 h-4 mr-1" />
                  Додај
                </button>
              ) : (
                <span className="text-sm text-gray-500 capitalize">
                  {user.friendship_status === 'pending' ? 'Чека одобрување' :
                   user.friendship_status === 'accepted' ? 'Пријател' : user.friendship_status}
                </span>
              )}
              <button
                onClick={() => onViewProfile(user.id)}
                className="px-3 py-1 border border-indigo-600 text-indigo-600 rounded-lg hover:bg-indigo-50 text-sm"
              >
                Профил
              </button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

// Main App компонента
const EventSocialNetwork: React.FC = () => {
  const [activeTab, setActiveTab] = useState<string>('discover');
  const [darkMode, setDarkMode] = useState<boolean>(() => getStoredTheme());
  const [user, setUser] = useState<User | null>(null);
  const [accessToken, setAccessToken] = useState<string | null>(() => getStoredToken());
  const [events, setEvents] = useState<Event[]>([]);
  const [venues, setVenues] = useState<VenuesMap>({});
  const [recommendations, setRecommendations] = useState<Event[]>([]);
  const [favorites, setFavorites] = useState<Event[]>([]);
  const [attended, setAttended] = useState<Event[]>([]);
  const [friends, setFriends] = useState<Friend[]>([]);
  const [socialFeed, setSocialFeed] = useState<any[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [selectedProfileId, setSelectedProfileId] = useState<number | null>(null);
  const [groupRecommendations, setGroupRecommendations] = useState<Event[]>([]);
  const [selectedFriendsForGroup, setSelectedFriendsForGroup] = useState<number[]>([]);

  const [loginForm, setLoginForm] = useState<LoginForm>({ email: '', password: '' });
  const [showLogin, setShowLogin] = useState<boolean>(!accessToken);

  const [filters] = useState<Filters>({
    start: '',
    end: '',
    city: '',
    tags: '',
    search: ''
  });

  // Dark mode effect
  useEffect(() => {
    if (darkMode) {
      document.documentElement.classList.add('dark');
    } else {
      document.documentElement.classList.remove('dark');
    }
    setStoredTheme(darkMode);
  }, [darkMode]);

  // Auto-load data when token exists
  useEffect(() => {
    if (accessToken) {
      loadEvents();
      loadRecommendations();
      loadFriends();
      loadFavorites();
      loadAttended();
      loadSocialFeed();
    }
  }, [accessToken]);

  const apiCall = async (endpoint: string, options: RequestInit = {}): Promise<any> => {
    const headers: HeadersInit = { 'Content-Type': 'application/json' };
    if (accessToken) {
      (headers as any).Authorization = `Bearer ${accessToken}`;
    }

    const response = await fetch(`${BACKEND_BASE}${endpoint}`, {
      headers,
      ...options
    });

    if (!response.ok) throw new Error(`API error: ${response.status}`);
    return response.json();
  };

  const login = async (e: React.MouseEvent<HTMLButtonElement> | React.KeyboardEvent<HTMLInputElement>): Promise<void> => {
    e.preventDefault();
    if (!loginForm.email || !loginForm.password) return;

    try {
      setLoading(true);
      const response = await apiCall('/api/auth/login', {
        method: 'POST',
        body: JSON.stringify(loginForm)
      });

      setAccessToken(response.access_token);
      setStoredToken(response.access_token);
      setUser(response.user);
      setShowLogin(false);
    } catch (error: any) {
      alert('Грешка при најава: ' + error.message);
    } finally {
      setLoading(false);
    }
  };

  const logout = (): void => {
    setAccessToken(null);
    setStoredToken(null);
    setUser(null);
    setShowLogin(true);
    setEvents([]);
    setVenues({});
    setRecommendations([]);
    setFavorites([]);
    setAttended([]);
    setFriends([]);
    setSocialFeed([]);
  };

  const loadEvents = async (): Promise<void> => {
    setLoading(true);
    try {
      const params = new URLSearchParams();
      Object.entries(filters).forEach(([key, value]: [string, string]) => {
        if (value) params.append(key, value);
      });

      const eventsData = await apiCall(`/api/events?${params}`);
      setEvents(Array.isArray(eventsData) ? eventsData : []);

      const venuesData = await apiCall('/api/venues');
      const venuesMap: VenuesMap = {};
      if (Array.isArray(venuesData)) {
        venuesData.forEach((v: Venue) => venuesMap[v.id] = v);
      }
      setVenues(venuesMap);
    } catch (error: any) {
      console.error('Error loading events:', error);
      setEvents([]);
    } finally {
      setLoading(false);
    }
  };

  const loadRecommendations = async (): Promise<void> => {
    try {
      // Земи СИТЕ настани (548), не само топ 100!
      const recs = await apiCall('/api/recommend/me?limit=10000');
      setRecommendations(Array.isArray(recs) ? recs : []);
    } catch (error: any) {
      console.error('Error loading recommendations:', error);
      setRecommendations([]);
    }
  };

  const loadFavorites = async (): Promise<void> => {
    try {
      const favs = await apiCall('/api/user/favorites');
      setFavorites(Array.isArray(favs) ? favs : []);
    } catch (error: any) {
      console.error('Error loading favorites:', error);
      setFavorites([]);
    }
  };

  const loadAttended = async (): Promise<void> => {
    try {
      const att = await apiCall('/api/user/attended');
      setAttended(Array.isArray(att) ? att : []);
    } catch (error: any) {
      console.error('Error loading attended:', error);
      setAttended([]);
    }
  };

  const loadFriends = async (): Promise<void> => {
    try {
      const friendsData = await apiCall('/api/friends/list');
      setFriends(Array.isArray(friendsData) ? friendsData : []);
    } catch (error: any) {
      console.error('Error loading friends:', error);
      setFriends([]);
    }
  };

  const loadSocialFeed = async (): Promise<void> => {
    try {
      const feed = await apiCall('/api/feed');
      setSocialFeed(Array.isArray(feed) ? feed : []);
    } catch (error: any) {
      console.error('Error loading social feed:', error);
      setSocialFeed([]);
    }
  };

  const handleSearch = async (query: string, searchFilters: Partial<Filters>): Promise<void> => {
    setLoading(true);
    try {
      const params = new URLSearchParams();
      if (query) params.append('q', query);

      Object.entries(searchFilters).forEach(([key, value]) => {
        if (value) params.append(key, value);
      });

      // Fixed: Use correct endpoint /api/events instead of /api/search/events
      const searchResults = await apiCall(`/api/events?${params}`);
      setEvents(Array.isArray(searchResults) ? searchResults : []);
    } catch (error: any) {
      console.error('Error searching events:', error);
      setEvents([]);
    } finally {
      setLoading(false);
    }
  };



  const handleToggleFavorite = async (eventId: number): Promise<void> => {
    try {
      // Провери дали настанот е веќе лајкан
      const event = events.find(e => e.id === eventId) || recommendations.find(e => (e.id || e.event_id) === eventId);
      const currentRating = event?.my_rating || 0;

      // Toggle: ако е лајкан (1), стави neutral (0), ако не е, стави like (1)
      const newRating = currentRating === 1 ? 0 : 1;

      await apiCall(`/api/events/${eventId}/rate`, {
        method: 'POST',
        body: JSON.stringify({ rating: newRating })
      });

      // ВАЖНО: Ажурирај my_rating локално во СИТЕ states за моментална визуелна feedback
      setEvents(prevEvents =>
        prevEvents.map(e => e.id === eventId ? { ...e, my_rating: newRating } : e)
      );
      setRecommendations(prevRecs =>
        prevRecs.map(e => (e.id === eventId || e.event_id === eventId) ? { ...e, my_rating: newRating } : e)
      );
      setFavorites(prevFavs =>
        prevFavs.map(e => e.id === eventId ? { ...e, my_rating: newRating } : e)
      );
      setGroupRecommendations(prevGroup =>
        prevGroup.map(e => (e.id === eventId || e.event_id === eventId) ? { ...e, my_rating: newRating } : e)
      );

      // Refresh data од server за да ги земеме новите скорови
      loadFavorites();
      loadEvents();
      loadRecommendations(); // Важно! Треба да се ажурираат препораките
      loadSocialFeed();
    } catch (error: any) {
      console.error('Error toggling favorite:', error);
      throw error;
    }
  };

  const handleToggleAttended = async (eventId: number): Promise<void> => {
    try {
      await apiCall(`/api/events/${eventId}/toggle-attended`, {
        method: 'PATCH'
      });

      // Refresh data
      loadAttended();
      loadEvents();
      loadSocialFeed();
    } catch (error: any) {
      console.error('Error toggling attended:', error);
      throw error;
    }
  };

  const handleDislike = async (eventId: number): Promise<void> => {
    try {
      // Провери дали настанот е веќе дислајкан
      const event = events.find(e => e.id === eventId) || recommendations.find(e => (e.id || e.event_id) === eventId);
      const currentRating = event?.my_rating || 0;

      // Toggle: ако е дислајкан (-1), стави neutral (0), ако не е, стави dislike (-1)
      const newRating = currentRating === -1 ? 0 : -1;

      await apiCall(`/api/events/${eventId}/rate`, {
        method: 'POST',
        body: JSON.stringify({ rating: newRating })
      });

      // ВАЖНО: Ажурирај my_rating локално во СИТЕ states за моментална визуелна feedback
      setEvents(prevEvents =>
        prevEvents.map(e => e.id === eventId ? { ...e, my_rating: newRating } : e)
      );
      setRecommendations(prevRecs =>
        prevRecs.map(e => (e.id === eventId || e.event_id === eventId) ? { ...e, my_rating: newRating } : e)
      );
      setFavorites(prevFavs =>
        prevFavs.map(e => e.id === eventId ? { ...e, my_rating: newRating } : e)
      );
      setGroupRecommendations(prevGroup =>
        prevGroup.map(e => (e.id === eventId || e.event_id === eventId) ? { ...e, my_rating: newRating } : e)
      );

      // Refresh data од server за да ги земеме новите скорови
      loadEvents();
      loadRecommendations(); // Ажурирај препораки
      loadSocialFeed();
    } catch (error: any) {
      console.error('Error disliking event:', error);
      throw error;
    }
  };

  const handleAddFriend = async (userId: number): Promise<void> => {
    try {
      await apiCall('/api/friends/request', {
        method: 'POST',
        body: JSON.stringify({ user_id: userId })
      });

      alert('Барањето за пријателство е испратено!');
    } catch (error: any) {
      console.error('Error adding friend:', error);
      alert('Грешка при додавање на пријател');
    }
  };

  const loadGroupRecommendations = async (friendIds: number[]): Promise<void> => {
    if (friendIds.length === 0) {
      setGroupRecommendations([]);
      return;
    }

    setLoading(true);
    try {
      const userIds = [user?.id, ...friendIds].filter(Boolean);
      const recs = await apiCall('/api/recommend/group', {
        method: 'POST',
        body: JSON.stringify({ user_ids: userIds })
      });
      setGroupRecommendations(Array.isArray(recs) ? recs : []);
    } catch (error: any) {
      console.error('Error loading group recommendations:', error);
      setGroupRecommendations([]);
    } finally {
      setLoading(false);
    }
  };

  const toggleFriendSelection = (friendId: number): void => {
    setSelectedFriendsForGroup(prev => {
      if (prev.includes(friendId)) {
        const newSelection = prev.filter(id => id !== friendId);
        loadGroupRecommendations(newSelection);
        return newSelection;
      } else {
        const newSelection = [...prev, friendId];
        loadGroupRecommendations(newSelection);
        return newSelection;
      }
    });
  };

  if (showLogin) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-indigo-500 via-purple-500 to-pink-500 flex items-center justify-center p-4">
        <div className="bg-white dark:bg-gray-800 rounded-2xl shadow-2xl p-8 w-full max-w-md">
          <div className="text-center mb-8">
            <div className="w-16 h-16 bg-gradient-to-r from-indigo-500 to-purple-600 rounded-full mx-auto mb-4 flex items-center justify-center">
              <Calendar className="w-8 h-8 text-white" />
            </div>
            <h1 className="text-3xl font-bold text-gray-900 dark:text-white mb-2">
              EventConnect
            </h1>
            <p className="text-gray-600 dark:text-gray-400">
              Твојата социјална мрежа за настани
            </p>
          </div>

          <div className="space-y-6">
            <div>
              <input
                type="email"
                placeholder="Email адреса"
                value={loginForm.email}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) => setLoginForm({...loginForm, email: e.target.value})}
                className="w-full p-4 border border-gray-300 rounded-xl focus:ring-2 focus:ring-indigo-500 focus:border-transparent"
                onKeyDown={(e: React.KeyboardEvent<HTMLInputElement>) => e.key === 'Enter' && login(e)}
              />
            </div>
            <div>
              <input
                type="password"
                placeholder="Лозинка"
                value={loginForm.password}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) => setLoginForm({...loginForm, password: e.target.value})}
                className="w-full p-4 border border-gray-300 rounded-xl focus:ring-2 focus:ring-indigo-500 focus:border-transparent"
                onKeyDown={(e: React.KeyboardEvent<HTMLInputElement>) => e.key === 'Enter' && login(e)}
              />
            </div>
            <button
              onClick={(e: React.MouseEvent<HTMLButtonElement>) => login(e)}
              disabled={loading}
              className={`w-full bg-gradient-to-r from-indigo-600 to-purple-600 text-white py-4 rounded-xl font-semibold hover:from-indigo-700 hover:to-purple-700 transition-all duration-200 shadow-lg ${
                loading ? 'opacity-50 cursor-not-allowed' : ''
              }`}
            >
              {loading ? 'Се најавувам...' : 'Најави се'}
            </button>
          </div>

          <div className="mt-6 text-center">
            <p className="text-sm text-gray-600">
              Тест корисници: <br />
              martin.stamenov03@gmail.com / test123<br />
              teodorasaneva@gmail.com / test123
            </p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className={`min-h-screen bg-gray-50 dark:bg-gray-900 transition-colors duration-200 ${darkMode ? 'dark' : ''}`}>
      <header className="bg-white dark:bg-gray-800 shadow-lg border-b border-gray-200 dark:border-gray-700">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            <div className="flex items-center">
              <div className="w-10 h-10 bg-gradient-to-r from-indigo-500 to-purple-600 rounded-lg flex items-center justify-center mr-3">
                <Calendar className="w-6 h-6 text-white" />
              </div>
              <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
                EventConnect
              </h1>
            </div>

            <div className="flex items-center space-x-4">
              <button
                onClick={() => setDarkMode(!darkMode)}
                className="p-2 text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg transition-colors"
                title={darkMode ? 'Light Mode' : 'Dark Mode'}
              >
                {darkMode ? '☀️' : '🌙'}
              </button>

              <Notifications accessToken={accessToken} />

              <div className="flex items-center space-x-2">
                <User className="w-5 h-5 text-gray-600 dark:text-gray-400" />
                <span className="text-sm text-gray-700 dark:text-gray-300">
                  {user?.name || 'User'}
                </span>
                <button
                  onClick={logout}
                  className="text-sm text-red-600 hover:text-red-700 ml-2"
                  title="Одјави се"
                >
                  <LogOut className="w-4 h-4" />
                </button>
              </div>
            </div>
          </div>
        </div>
      </header>

      <nav className="bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700">
        <div className="max-w-7xl mx-auto px-4">
          <div className="flex space-x-8">
            {[
              { key: 'discover', label: 'Откријте', icon: Search },
              { key: 'calendar', label: 'Календар', icon: Calendar },
              { key: 'recommendations', label: 'Препораки', icon: Star },
              { key: 'group', label: 'Групни Препораки', icon: Users },
              { key: 'favorites', label: 'Омилени', icon: Heart },
              { key: 'attended', label: 'Присуствувал', icon: CheckCircle },
              { key: 'friends', label: 'Пријатели', icon: Users },
              { key: 'feed', label: 'Ѕид', icon: MessageCircle }
            ].map(({ key, label, icon: Icon }) => (
              <button
                key={key}
                onClick={() => setActiveTab(key)}
                className={`flex items-center px-4 py-4 text-sm font-medium border-b-2 transition-colors ${
                  activeTab === key
                    ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400'
                    : 'border-transparent text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200'
                }`}
              >
                <Icon className="w-4 h-4 mr-2" />
                {label}
              </button>
            ))}
          </div>
        </div>
      </nav>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {activeTab === 'discover' && (
          <div>
            <SearchComponent onSearch={handleSearch} loading={loading} />

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {loading ? (
                Array.from({length: 6}, (_, i: number) => (
                  <div key={i} className="bg-white dark:bg-gray-800 rounded-xl shadow-lg h-64 animate-pulse"></div>
                ))
              ) : events.length > 0 ? (
                events.map((event: Event) => (
                  <EventCard
                    key={event.id || event.event_id || Math.random()}
                    event={event}
                    venue={event.venue_id ? venues[event.venue_id] : undefined}
                    onToggleFavorite={handleToggleFavorite}
                    onToggleAttended={handleToggleAttended}
                    onDislike={handleDislike}
                  />
                ))
              ) : (
                <div className="col-span-full text-center py-12">
                  <p className="text-gray-500 dark:text-gray-400">Нема пронајдени настани.</p>
                </div>
              )}
            </div>
          </div>
        )}

        {activeTab === 'calendar' && (
          <CalendarView events={events} venues={venues} />
        )}

        {activeTab === 'recommendations' && (
          <div>
            <div className="flex items-center justify-between mb-6">
              <h2 className="text-2xl font-bold text-gray-900 dark:text-white">
                Препораки за тебе
              </h2>
              <button
                onClick={loadRecommendations}
                disabled={loading}
                className={`bg-indigo-600 text-white px-4 py-2 rounded-lg hover:bg-indigo-700 transition-colors ${
                  loading ? 'opacity-50 cursor-not-allowed' : ''
                }`}
              >
                Освежи
              </button>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {recommendations.length > 0 ? (
                recommendations.map((event: Event) => (
                  <EventCard
                    key={event.event_id || event.id || Math.random()}
                    event={event}
                    venue={event.venue_id ? venues[event.venue_id] : undefined}
                    onToggleFavorite={handleToggleFavorite}
                    onToggleAttended={handleToggleAttended}
                    onDislike={handleDislike}
                    showRecommendation={true}
                  />
                ))
              ) : (
                <div className="col-span-full text-center py-12">
                  <p className="text-gray-500 dark:text-gray-400">Нема препораки за прикажување.</p>
                </div>
              )}
            </div>
          </div>
        )}

        {activeTab === 'favorites' && (
          <div>
            <div className="flex items-center justify-between mb-6">
              <h2 className="text-2xl font-bold text-gray-900 dark:text-white">
                Омилени настани ({favorites.length})
              </h2>
              <button
                onClick={loadFavorites}
                className="bg-indigo-600 text-white px-4 py-2 rounded-lg hover:bg-indigo-700 transition-colors"
              >
                Освежи
              </button>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {favorites.length > 0 ? (
                favorites.map((event: Event) => (
                  <EventCard
                    key={event.id || Math.random()}
                    event={event}
                    venue={event.venue_id ? venues[event.venue_id] : undefined}
                    onToggleFavorite={handleToggleFavorite}
                    onToggleAttended={handleToggleAttended}
                    onDislike={handleDislike}
                    showActions={true}
                  />
                ))
              ) : (
                <div className="col-span-full text-center py-12">
                  <Heart className="w-16 h-16 text-gray-300 mx-auto mb-4" />
                  <p className="text-gray-500 dark:text-gray-400">Нема омилени настани.</p>
                  <p className="text-gray-400 text-sm mt-2">Лајкај настани за да ги видиш тука!</p>
                </div>
              )}
            </div>
          </div>
        )}

        {activeTab === 'attended' && (
          <div>
            <div className="flex items-center justify-between mb-6">
              <h2 className="text-2xl font-bold text-gray-900 dark:text-white">
                Присуствувал ({attended.length})
              </h2>
              <button
                onClick={loadAttended}
                className="bg-indigo-600 text-white px-4 py-2 rounded-lg hover:bg-indigo-700 transition-colors"
              >
                Освежи
              </button>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {attended.length > 0 ? (
                attended.map((event: Event) => (
                  <EventCard
                    key={event.id || Math.random()}
                    event={event}
                    venue={event.venue_id ? venues[event.venue_id] : undefined}
                    onToggleFavorite={handleToggleFavorite}
                    onToggleAttended={handleToggleAttended}
                    onDislike={handleDislike}
                    showActions={true}
                  />
                ))
              ) : (
                <div className="col-span-full text-center py-12">
                  <CheckCircle className="w-16 h-16 text-gray-300 mx-auto mb-4" />
                  <p className="text-gray-500 dark:text-gray-400">Не сте присуствувале на настани.</p>
                  <p className="text-gray-400 text-sm mt-2">Означете настани каде сте биле!</p>
                </div>
              )}
            </div>
          </div>
        )}

        {activeTab === 'friends' && (
          <div className="space-y-6">
            <div>
              <h2 className="text-2xl font-bold text-gray-900 dark:text-white mb-6">
                Пријатели ({friends.length})
              </h2>

              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
                {friends.length > 0 ? (
                  friends.map((friend: Friend) => (
                    <div key={friend.id || Math.random()} className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-6 cursor-pointer hover:shadow-xl transition-shadow"
                         onClick={() => setSelectedProfileId(friend.id)}>
                      <div className="flex items-center justify-between">
                        <div className="flex items-center">
                          <div className="w-12 h-12 bg-gradient-to-r from-indigo-500 to-purple-600 rounded-full flex items-center justify-center mr-4">
                            <User className="w-6 h-6 text-white" />
                          </div>
                          <div>
                            <h3 className="font-semibold text-gray-900 dark:text-white">
                              {friend.name || 'Без име'}
                            </h3>
                            <p className="text-sm text-gray-600 dark:text-gray-400">
                              {friend.city || 'Без локација'}
                            </p>
                          </div>
                        </div>
                        <span className="text-gray-400">→</span>
                      </div>
                    </div>
                  ))
                ) : (
                  <div className="col-span-full text-center py-12">
                    <Users className="w-16 h-16 text-gray-300 mx-auto mb-4" />
                    <p className="text-gray-500 dark:text-gray-400">Нема пријатели.</p>
                    <p className="text-gray-400 text-sm mt-2">Пребарајте и додајте пријатели подолу!</p>
                  </div>
                )}
              </div>
            </div>

            <FriendsSearch onAddFriend={handleAddFriend} onViewProfile={setSelectedProfileId} />
          </div>
        )}

        {activeTab === 'feed' && (
          <div>
            <div className="flex items-center justify-between mb-6">
              <h2 className="text-2xl font-bold text-gray-900 dark:text-white">
                Социјален ѕид
              </h2>
              <button
                onClick={loadSocialFeed}
                className="bg-indigo-600 text-white px-4 py-2 rounded-lg hover:bg-indigo-700 transition-colors"
              >
                Освежи
              </button>
            </div>

            <div className="space-y-4">
              {socialFeed.length > 0 ? (
                socialFeed.map((activity, index) => (
                  <div key={index} className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-6">
                    <div className="flex items-start space-x-4">
                      <div className="w-10 h-10 bg-gradient-to-r from-indigo-500 to-purple-600 rounded-full flex items-center justify-center">
                        <User className="w-6 h-6 text-white" />
                      </div>
                      <div className="flex-1">
                        <div className="flex items-center space-x-2 mb-2">
                          <span className="font-semibold text-gray-900 dark:text-white">
                            {activity.user.name}
                          </span>
                          <span className="text-gray-500 dark:text-gray-400">
                            {activity.action === 'liked' ? 'му се допаѓа' :
                             activity.action === 'attended' ? 'присуствуваше на' :
                             'не му се допаѓа'} настанот
                          </span>
                        </div>
                        <div className="bg-gray-50 dark:bg-gray-700 rounded-lg p-4">
                          <h4 className="font-semibold text-gray-900 dark:text-white">
                            {activity.event.title}
                          </h4>
                          <div className="flex items-center text-sm text-gray-600 dark:text-gray-400 mt-1">
                            <MapPin className="w-4 h-4 mr-1" />
                            <span>{activity.event.venue}</span>
                            <span className="mx-2">•</span>
                            <Clock className="w-4 h-4 mr-1" />
                            <span>{formatEventDate(activity.event.starts_at).relative}</span>
                          </div>
                        </div>
                        <div className="text-xs text-gray-400 mt-2">
                          {new Date(activity.timestamp).toLocaleString('mk-MK')}
                        </div>
                      </div>
                    </div>
                  </div>
                ))
              ) : (
                <div className="text-center py-12">
                  <MessageCircle className="w-16 h-16 text-gray-300 mx-auto mb-4" />
                  <p className="text-gray-500 dark:text-gray-400">Нема активности.</p>
                  <p className="text-gray-400 text-sm mt-2">Додајте пријатели за да видите нивни активности!</p>
                </div>
              )}
            </div>
          </div>
        )}

        {activeTab === 'group' && (
          <div>
            <div className="mb-6">
              <h2 className="text-2xl font-bold text-gray-900 dark:text-white mb-2">
                🎉 Групни Препораки
              </h2>
              <p className="text-gray-600 dark:text-gray-400">
                Избери пријатели и пронајди настани што ќе ви се допаднат на СИТЕ!
              </p>
            </div>

            {friends.length === 0 ? (
              <div className="text-center py-12">
                <Users className="w-16 h-16 text-gray-300 mx-auto mb-4" />
                <p className="text-gray-500 dark:text-gray-400">Нема пријатели.</p>
                <p className="text-gray-400 text-sm mt-2">Додајте пријатели за да користите групни препораки!</p>
              </div>
            ) : (
              <div>
                <div className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-6 mb-6">
                  <h3 className="font-semibold text-gray-900 dark:text-white mb-4">
                    Избери пријатели ({selectedFriendsForGroup.length} избрани)
                  </h3>
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                    {friends.map((friend: Friend) => {
                      const isSelected = selectedFriendsForGroup.includes(friend.id);
                      return (
                        <div
                          key={friend.id}
                          onClick={() => toggleFriendSelection(friend.id)}
                          className={`flex items-center p-4 rounded-lg border-2 cursor-pointer transition-all ${
                            isSelected
                              ? 'border-indigo-500 bg-indigo-50 dark:bg-indigo-900'
                              : 'border-gray-200 dark:border-gray-700 hover:border-indigo-300'
                          }`}
                        >
                          <div className={`w-10 h-10 rounded-full flex items-center justify-center mr-3 ${
                            isSelected ? 'bg-indigo-500' : 'bg-gray-300'
                          }`}>
                            {isSelected ? (
                              <CheckCircle className="w-6 h-6 text-white" />
                            ) : (
                              <User className="w-6 h-6 text-gray-600" />
                            )}
                          </div>
                          <div className="flex-1">
                            <div className="font-semibold text-gray-900 dark:text-white">
                              {friend.name}
                            </div>
                            <div className="text-sm text-gray-500">{friend.city}</div>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>

                {selectedFriendsForGroup.length > 0 && (
                  <div>
                    <div className="flex items-center justify-between mb-4">
                      <h3 className="text-xl font-bold text-gray-900 dark:text-white">
                        Препораки за {selectedFriendsForGroup.length + 1} {selectedFriendsForGroup.length === 0 ? 'корисник' : 'корисници'}
                      </h3>
                      {loading && <span className="text-sm text-gray-500">Вчитувам...</span>}
                    </div>

                    {groupRecommendations.length > 0 ? (
                      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                        {groupRecommendations.map((event: any) => (
                          <div key={event.event_id} className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-6 border-2 border-indigo-200 dark:border-indigo-800">
                            <div className="flex items-center justify-between mb-3">
                              <div className="text-sm font-medium text-indigo-600 dark:text-indigo-400">
                                Групен Скор
                              </div>
                              <div className="text-2xl font-bold text-indigo-600 dark:text-indigo-400">
                                {event.group_score_pct}%
                              </div>
                            </div>

                            <h4 className="font-bold text-lg text-gray-900 dark:text-white mb-2">
                              {event.title}
                            </h4>

                            <div className="flex items-center text-sm text-gray-600 dark:text-gray-400 mb-3">
                              <Calendar className="w-4 h-4 mr-2" />
                              <span>{new Date(event.starts_at).toLocaleDateString('mk-MK')}</span>
                            </div>

                            {event.tags && event.tags.length > 0 && (
                              <div className="flex flex-wrap gap-2">
                                {event.tags.slice(0, 3).map((tag: string, i: number) => (
                                  <span key={i} className="px-2 py-1 bg-indigo-100 dark:bg-indigo-900 text-indigo-800 dark:text-indigo-200 text-xs rounded-full">
                                    {tag}
                                  </span>
                                ))}
                              </div>
                            )}

                            <div className="mt-4 pt-4 border-t border-gray-200 dark:border-gray-700">
                              <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2">
                                <div
                                  className="h-2 rounded-full bg-gradient-to-r from-indigo-500 to-purple-600"
                                  style={{ width: `${event.group_score_pct}%` }}
                                ></div>
                              </div>
                              <p className="text-xs text-gray-500 mt-2 text-center">
                                Сите ќе уживаат во овој настан!
                              </p>
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : (
                      !loading && (
                        <div className="text-center py-12 bg-gray-50 dark:bg-gray-800 rounded-xl">
                          <Star className="w-16 h-16 text-gray-300 mx-auto mb-4" />
                          <p className="text-gray-500 dark:text-gray-400">Нема препораки.</p>
                          <p className="text-gray-400 text-sm mt-2">Избери повеќе пријатели или променете ги филтрите.</p>
                        </div>
                      )
                    )}
                  </div>
                )}
              </div>
            )}
          </div>
        )}
      </main>

      {/* Profile Modal */}
      {selectedProfileId && (
        <UserProfileModal
          userId={selectedProfileId}
          onClose={() => setSelectedProfileId(null)}
          accessToken={accessToken}
        />
      )}
    </div>
  );
};

export default EventSocialNetwork;