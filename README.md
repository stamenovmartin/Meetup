# 🎯 Meetup Event Recommendation System with GNN

**Напреден препорачувачки систем за настани со Graph Neural Networks (GNN)**

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Flask](https://img.shields.io/badge/Flask-3.0+-green.svg)](https://flask.palletsprojects.com/)
[![React](https://img.shields.io/badge/React-18+-blue.svg)](https://reactjs.org/)
[![PyTorch](https://img.shields.io/badge/PyTorch-2.0+-red.svg)](https://pytorch.org/)
[![GNN](https://img.shields.io/badge/GNN-GraphSAGE-orange.svg)](https://pytorch-geometric.readthedocs.io/)

---

## 📖 За Проектот

Ова е **full-stack event recommendation platform** што комбинира традиционални препорачувачки техники со модерни **Graph Neural Networks (GNN)** за персонализирани препораки на настани.

### 🔥 Клучни Функционалности

- **🤖 Hybrid GNN Recommendation System**
  - Традиционални features (тагови, геолокација)
  - GNN embeddings (GraphSAGE) за event similarity
  - Интелигентно балансирање (likes ← GNN помага, dislikes ← GNN ignored)

- **⚡ Real-time User Feedback**
  - Like/Dislike систем со моментална синхронизација
  - Cross-tab state management
  - Автоматско ажурирање на препораки

- **📊 Advanced Analytics**
  - 5 типови на визуелизации (score distribution, GNN vs traditional, tag analysis, user behavior, quality metrics)
  - Автоматски генерирани извештаи
  - Детална статистика

- **🌐 Social Network Features**
  - Group препораки
  - Комплексен social feed
  - Event sharing

---

## 🏗️ Структура на Проектот

```
Meetup-master/
│
├── backend/                      # (future: организирани backend компоненти)
│
├── frontend/                     # React + TypeScript + Vite
│   ├── src/
│   │   ├── EventSocialNetwork.tsx    # Главна компонента
│   │   ├── EventCard.tsx             # Event карти со Like/Dislike
│   │   └── ...
│   └── package.json
│
├── models/                       # Core ML models
│   ├── recommender.py            # GNN препорачувач (главен!)
│   ├── db_models.py              # SQLAlchemy database models
│   └── gnn_training.py           # GNN тренинг скриптови
│
├── graph_construction/           # GNN graph creation
│   ├── graph_construction.py         # Graph builder
│   ├── enhanced_graph_construction.py
│   └── graph_data/                   # Генерирани .pt фајлови
│
├── data_collection/              # Web scraping & data processing
│   ├── scrapers/                     # Event scrapers (all_events, karti.com.mk, itevents.mk, cineplexx)
│   └── NLP_data/                     # Data cleaning & preprocessing
│
├── scripts/                      # Utility scripts
│   ├── gnn_analytics_visualizations.py   # 📊 GNN Analytics (NEW!)
│   ├── ingest_csv.py                     # Data import
│   └── ...
│
├── docs/                         # Documentation
│   ├── analytics/                    # Генерирани слики од GNN analytics
│   │   ├── 01_score_distribution.png
│   │   ├── 02_gnn_vs_traditional.png
│   │   ├── 03_tag_distribution.png
│   │   ├── 04_user_behavior.png
│   │   └── 05_recommendation_quality.png
│   └── legacy_fixes/                 # Архивирани debugging MD фајлови
│
├── instance/                     # Database & instance files
│   └── events.db                     # SQLite database
│
├── templates/                    # Flask templates (legacy)
│
├── main.py                       # Flask backend API
├── add_tags_to_events.py         # Tag generation utility
├── requirements.txt              # Python dependencies
├── package.json                  # Node dependencies
└── README.md                     # Ова е документот што го читаш!
```

---

## 🚀 Како да го Стартираш Проектот

### Предуслови

```bash
# Python 3.10+
python --version

# Node.js 18+
node --version
npm --version
```

### 1️⃣ Clone Repository

```bash
cd Desktop
# (already cloned as Meetup-master/)
cd Meetup-master
```

### 2️⃣ Setup Backend

```bash
# Креирај virtual environment
python -m venv .venv

# Активирај го (Windows)
.venv\Scripts\activate

# Инсталирај dependencies
pip install -r requirements.txt
```

### 3️⃣ Setup Frontend

```bash
cd frontend
npm install
cd ..
```

### 4️⃣ Стартирај Апликацијата

**Terminal 1 - Backend (Flask):**
```bash
python main.py
```
→ Порти: `http://localhost:5000`

**Terminal 2 - Frontend (React):**
```bash
cd frontend
npm run dev
```
→ Порти: `http://localhost:5173`

### 5️⃣ Логирај се

Отвори `http://localhost:5173` во browser и логирај се:

- **Email:** `martin.stamenov03@gmail.com`
- **Password:** `test123`

---

## 📊 GNN Analytics & Visualizations

### Генерирање на Аналитики

Проектот вклучува напреден analytics скрипт со 5 типови на визуелизации:

```bash
# Стартирај analytics за User ID 1
python scripts/gnn_analytics_visualizations.py 1

# За друг user
python scripts/gnn_analytics_visualizations.py <USER_ID>
```

### Што ги генерира?

1. **📈 Score Distribution** (`01_score_distribution.png`)
   - Histogram, cumulative distribution, box plot
   - Raw score vs normalized score

2. **⚖️ GNN vs Traditional Scoring** (`02_gnn_vs_traditional.png`)
   - Scatter plot comparison
   - Distribution overlap
   - Correlation analysis

3. **🏷️ Tag Distribution** (`03_tag_distribution.png`)
   - Top 15 tags во препораки
   - Pie chart (top 10)

4. **👤 User Behavior** (`04_user_behavior.png`)
   - Likes vs Dislikes
   - Liked/Disliked tags
   - Preference ratios

5. **✅ Recommendation Quality** (`05_recommendation_quality.png`)
   - Score decay curve
   - Decile analysis
   - Quality metrics summary

**Слики се зачувуваат во:** `docs/analytics/`

---

## 🧠 Како Работи GNN Системот

### Архитектура

```
User Input (Like/Dislike)
         ↓
┌────────────────────────────────────────────────┐
│ 1. Traditional Features                        │
│    • Tag matching (likes ← +1, dislikes ← -5)  │
│    • Geo-location distance                     │
│    → Traditional Score                          │
└────────────────────────────────────────────────┘
         ↓
┌────────────────────────────────────────────────┐
│ 2. GNN Features                                │
│    • Event embeddings (548 events, 59-dim)     │
│    • Cosine similarity со liked events          │
│    → GNN Similarity Score                       │
└────────────────────────────────────────────────┘
         ↓
┌────────────────────────────────────────────────┐
│ 3. Combined Scoring                            │
│                                                │
│   IF traditional_score < 0 (disliked):         │
│      → return traditional_score                 │
│      (GNN се игнорира!)                        │
│                                                │
│   ELSE (liked/neutral):                        │
│      → 0.6 × traditional + 0.4 × (GNN × 10)    │
│      (GNN активно помага)                       │
└────────────────────────────────────────────────┘
         ↓
┌────────────────────────────────────────────────┐
│ 4. Percentile Ranking                          │
│    • Сортирај по combined score                │
│    • Пресметај percentile (0-100%)             │
│    • rank=1 → 100%, rank=548 → 0%              │
└────────────────────────────────────────────────┘
         ↓
    Top 50 препораки
```

### Клучни Одлуки

1. **GNN се користи само за позитивни препораки**
   - Dislikes → GNN ignored (100% traditional)
   - Likes/Neutral → GNN активен (60/40 balance)

2. **Негативни тагови се 5× појаки од позитивни**
   - Dislike на "Music" → сите Music events паѓаат драстично

3. **Percentile scoring (0-100%)**
   - Праведна дистрибуција на скорови
   - rank 1/548 → 100%, rank 548/548 → 0%

---

## 🎨 Frontend Features

### Главни Табови

1. **🔍 Откријте (Discover)**
   - Сите настани со пребарување
   - Like/Dislike копчиња

2. **⭐ Препораки (Recommendations)**
   - GNN-базирани препораки (0-100% score)
   - Сортирани по квалитет

3. **❤️ Омилени (Favorites)**
   - Liked настани

4. **👥 Групни Препораки (Group)**
   - Social network препораки

### Like/Dislike Синхронизација

```typescript
// Кога кликнеш Like/Dislike, се ажурираат СИТЕ 4 state arrays:
setEvents(...)              // "Откријте" таб
setRecommendations(...)     // "Препораки" таб
setFavorites(...)           // "Омилени" таб
setGroupRecommendations(...) // "Групни" таб

// → Копчињата се синхронизирани насекаде! ✅
```

---

## 🧪 Тестирање

### Тест 1: Dislikes влијаат на скорови

```
1. Логирај се
2. Иди на "Откријте"
3. Дислајкај 10 "Music/Concert" настани
4. Рефреш (F5)
5. Иди на "Препораки"
6. Провери: Music events треба да имаат 0-20% scores ✅
```

### Тест 2: Синхронизација на копчиња

```
1. Like настан во "Откријте"
2. Иди на "Препораки" (без refresh)
3. Провери: Истиот настан, копчето треба да е ЦРВЕНО ✅
4. Click Like повторно во "Препораки"
5. Иди назад на "Откријте"
6. Провери: Копчето треба да е НЕАКТИВНО ✅
```

---

## 📦 Dependencies

### Backend (Python)

```txt
Flask==3.1.0
Flask-SQLAlchemy==3.1.1
Flask-JWT-Extended==4.7.1
Flask-CORS==5.0.0
torch==2.5.1
torch-geometric==2.6.1
numpy==2.2.3
requests==2.32.3
beautifulsoup4==4.12.3
matplotlib==3.10.0        # За analytics
seaborn==0.13.2           # За analytics
scipy==1.15.1             # За analytics
```

### Frontend (Node)

```json
{
  "react": "^18.3.1",
  "react-dom": "^18.3.1",
  "vite": "^6.0.11",
  "typescript": "~5.7.2"
}
```

---

## 🔧 Configuration

### Backend (.env)

```env
SECRET_KEY=your-secret-key
JWT_SECRET_KEY=your-jwt-secret
DATABASE_URI=sqlite:///instance/events.db
```

### GNN Graph Path

Graph фајлот се бара автоматски на:

1. `graph_construction/graph_data/event_similarity_graph.pt`
2. `gnn_results/models/GraphSAGE_event_similarity.pt`
3. `models/event_graph.pt`

---

## 📚 Документација

### Архивирани Debugging MD Фајлови

Сите legacy debugging фајлови се во `docs/legacy_fixes/`:

- `ALL_FIXES_COMPLETE.md`
- `COMPLETE_IMPROVEMENTS_SUMMARY.md`
- `FINAL_TEST_GUIDE.md`
- `FIXES_APPLIED.md`
- `GNN_VERIFICATION_REPORT.md`
- `IMPROVEMENTS_2025-09-29.md`
- `PROJECT_COMPLETE.md`
- `SCORING_COMPLETELY_FIXED.md`
- `SCORING_FIXED.md`
- `STATE_SYNC_FIX.md`
- `TEST_INSTRUCTIONS.md`

### Корисни Скриптови

```bash
# 1. Додади тагови на настани
python add_tags_to_events.py

# 2. Генерирај GNN analytics
python scripts/gnn_analytics_visualizations.py 1

# 3. Import events од CSV
python scripts/ingest_csv.py

# 4. Тренирај GNN модел
python models/gnn_training.py

# 5. Build GNN graph
python graph_construction/graph_construction.py
```

---

## 🐛 Troubleshooting

### Проблем: Backend не стартира

```bash
# Провери дали има грешки:
python main.py

# Ако има проблем со database:
rm instance/events.db
python scripts/ingest_csv.py
```

### Проблем: Frontend не се поврзува на backend

```bash
# Провери дали backend работи:
curl http://localhost:5000/api/health

# Провери CORS во main.py
```

### Проблем: GNN не работи

```bash
# Провери дали graph фајлот постои:
ls -lh graph_construction/graph_data/*.pt

# Тестирај GNN:
python -c "from models.recommender import get_recommender; r = get_recommender(); print('GNN OK!' if r.event_embeddings is not None else 'GNN FAIL!')"
```

---

## 🎯 Идни Подобрувања

- [ ] User profile preferences
- [ ] Advanced filtering (date, location, price)
- [ ] Email notifications за нови препораки
- [ ] Admin dashboard
- [ ] Multi-language support (EN/MK)
- [ ] Mobile app (React Native)
- [ ] A/B testing framework
- [ ] Real-time collaborative filtering

---

## 👨‍💻 Автор

**Martin Stamenov**
- Email: martin.stamenov03@gmail.com
- GitHub: [GitHub Profile]
- University Project: MSc Computer Science - GNN Recommendation Systems

---

## 📄 License

MIT License - слободно користење и модификација.

---

## 🙏 Acknowledgments

- **PyTorch Geometric** - GNN framework
- **Flask** - Backend framework
- **React** - Frontend framework
- **Claude AI** - Development assistant

---

**Последно ажурирано:** 2025-09-30

**Верзија:** 2.0.0 (GNN Fully Functional + Analytics)

---

## 💡 Quick Start Cheat Sheet

```bash
# 1. Setup
python -m venv .venv && .venv\Scripts\activate
pip install -r requirements.txt
cd frontend && npm install && cd ..

# 2. Run
python main.py                        # Terminal 1
cd frontend && npm run dev            # Terminal 2

# 3. Test
http://localhost:5173                 # Login: martin.stamenov03@gmail.com / test123

# 4. Analytics
python scripts/gnn_analytics_visualizations.py 1

# 5. Check results
open docs/analytics/*.png             # 5 visualization files
```

🎉 **Готово! Уживај во GNN-powered препораки!** 🚀
