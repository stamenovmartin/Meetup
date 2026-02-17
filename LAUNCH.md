# 🚀 LAUNCH GUIDE - EventConnect

## ⚡ QUICK START (5 Minutes)

### **STEP 1: Start Backend Server** 🖥️

Open **Command Prompt** or **PowerShell** and run:

```bash
cd C:\Users\Martin.DESKTOP-J36C0SU\Desktop\Meetup-master
python main.py
```

**What happens:**
- ✅ Creates database (`meetup.db`)
- ✅ Starts Flask server on `http://localhost:5000`
- ✅ Ready to accept API requests

**You should see:**
```
* Running on http://127.0.0.1:5000
* Debug mode: on
```

**⚠️ LEAVE THIS WINDOW OPEN!** (Don't close it)

---

### **STEP 2: Import Events to Database** 📊

Open a **NEW** Command Prompt window and run:

```bash
cd C:\Users\Martin.DESKTOP-J36C0SU\Desktop\Meetup-master
python scripts/ingest_csv.py
```

**What happens:**
- ✅ Imports 1,873 events from `events_gnn_ready.csv`
- ✅ Creates venues
- ✅ Populates database

**This takes 2-3 minutes**

**You should see:**
```
✅ Imported 1873 events
✅ Created 500+ venues
✅ Database ready!
```

---

### **STEP 3: Create Test Users** 👥

In the **SAME** Command Prompt window, run:

```bash
python
```

Then paste this code:

```python
from main import create_app
from models.db_models import db, User

app = create_app()
with app.app_context():
    # Create User 1
    u1 = User(email="martin.stamenov03@gmail.com", name="Martin", city="Скопје")
    u1.set_password("test123")
    db.session.add(u1)

    # Create User 2
    u2 = User(email="teodorasaneva@gmail.com", name="Teodora", city="Скопје")
    u2.set_password("test123")
    db.session.add(u2)

    # Create User 3
    u3 = User(email="demo@test.com", name="Demo User", city="Битола")
    u3.set_password("test123")
    db.session.add(u3)

    db.session.commit()
    print("✅ Created 3 test users!")
    print("   - martin.stamenov03@gmail.com / test123")
    print("   - teodorasaneva@gmail.com / test123")
    print("   - demo@test.com / test123")
```

Press **Enter** twice, then type `exit()` and press Enter.

---

### **STEP 4: Start Frontend** 🎨

Open **ANOTHER NEW** Command Prompt window and run:

```bash
cd C:\Users\Martin.DESKTOP-J36C0SU\Desktop\Meetup-master\frontend
npm install
```

**Wait for installation to complete (1-2 minutes)**

Then run:

```bash
npm run dev
```

**You should see:**
```
  ➜  Local:   http://localhost:5173/
  ➜  Network: use --host to expose
```

---

### **STEP 5: Open Your Browser** 🌐

Go to: **http://localhost:5173**

**Login with:**
- Email: `martin.stamenov03@gmail.com`
- Password: `test123`

---

## 🎉 **YOU'RE LIVE!**

You should now see the **EventConnect** homepage with:
- ✅ 1,873 events
- ✅ Search functionality
- ✅ Dark/Light mode toggle
- ✅ Navigation tabs

---

## 📖 **COMPLETE MANUAL LAUNCH** (If Quick Start Doesn't Work)

### **Prerequisites:**
- Python 3.10+ installed
- Node.js 18+ installed
- Git Bash or Command Prompt

---

### **1. Backend Setup (Terminal 1)**

```bash
# Navigate to project
cd C:\Users\Martin.DESKTOP-J36C0SU\Desktop\Meetup-master

# Install Python dependencies
pip install flask flask-cors flask-jwt-extended python-dotenv sqlalchemy pandas scikit-learn torch torch-geometric networkx matplotlib

# Start backend
python main.py
```

**Leave this running!**

---

### **2. Import Events (Terminal 2)**

```bash
cd C:\Users\Martin.DESKTOP-J36C0SU\Desktop\Meetup-master

# Check if CSV exists
dir data_collection\NLP_data\cleaned_data\events_gnn_ready.csv

# Import events
python
```

In Python console:
```python
from scripts.ingest_csv import ingest_from_csv
from main import create_app

app = create_app()
with app.app_context():
    stats = ingest_from_csv(
        "data_collection/NLP_data/cleaned_data/events_gnn_ready.csv",
        created_by_user_id=1,
        limit=None  # Import all
    )
    print(f"✅ Imported: {stats}")
```

Press Ctrl+Z then Enter to exit Python.

---

### **3. Create Users (Same Terminal)**

```bash
python
```

```python
from main import create_app
from models.db_models import db, User

app = create_app()
with app.app_context():
    # User 1
    if not User.query.filter_by(email="martin.stamenov03@gmail.com").first():
        u1 = User(email="martin.stamenov03@gmail.com", name="Martin", city="Скопје")
        u1.set_password("test123")
        db.session.add(u1)

    # User 2
    if not User.query.filter_by(email="teodorasaneva@gmail.com").first():
        u2 = User(email="teodorasaneva@gmail.com", name="Teodora", city="Скопје")
        u2.set_password("test123")
        db.session.add(u2)

    # User 3
    if not User.query.filter_by(email="demo@test.com").first():
        u3 = User(email="demo@test.com", name="Demo User", city="Битола")
        u3.set_password("test123")
        db.session.add(u3)

    db.session.commit()

    print("\n✅ Users created:")
    for u in User.query.all():
        print(f"   - {u.email} / test123")
```

Exit Python.

---

### **4. Frontend Setup (Terminal 3)**

```bash
cd C:\Users\Martin.DESKTOP-J36C0SU\Desktop\Meetup-master\frontend

# Install dependencies
npm install

# Start dev server
npm run dev
```

---

### **5. Open Browser**

Visit: **http://localhost:5173**

---

## 🧪 **TESTING THE FEATURES**

### **Test 1: Login**
- Email: `martin.stamenov03@gmail.com`
- Password: `test123`
- ✅ Should log you in

### **Test 2: Browse Events**
- Click "Откријте" tab
- ✅ Should see 1,873 events

### **Test 3: Search**
- Type "IT" in search box
- ✅ Should filter to IT events

### **Test 4: Like an Event**
- Click ❤️ on any event
- ✅ Should turn red
- Click "Омилени" tab
- ✅ Should see your liked events

### **Test 5: Get Recommendations**
- Like 3-5 events of different types
- Click "Препораки" tab
- ✅ Should see personalized recommendations

### **Test 6: Add a Friend**
- Click "Пријатели" tab
- Search for "Teodora"
- Click "Додај"
- ✅ Should send friend request

### **Test 7: Accept Friend Request**
- Open a **private/incognito** browser window
- Go to http://localhost:5173
- Login as `teodorasaneva@gmail.com` / `test123`
- Click the 🔔 notification bell
- Click "Прифати"
- ✅ Should accept friend request

### **Test 8: Group Recommendations** 🎯
- Go back to Martin's account
- Click "Групни Препораки" tab
- Select Teodora (checkbox)
- ✅ Should see events ranked by group score!

---

## 🛠️ **TROUBLESHOOTING**

### **Problem: Backend won't start**
**Error:** `ModuleNotFoundError: No module named 'flask'`

**Fix:**
```bash
pip install flask flask-cors flask-jwt-extended python-dotenv sqlalchemy
```

---

### **Problem: Can't import events**
**Error:** `FileNotFoundError`

**Fix:**
```bash
# Make sure CSV exists
cd C:\Users\Martin.DESKTOP-J36C0SU\Desktop\Meetup-master
dir data_collection\NLP_data\cleaned_data\events_gnn_ready.csv

# If not found, run data cleaning
cd data_collection\NLP_data
python data_cleaning_preprocessing.py
```

---

### **Problem: Frontend won't start**
**Error:** `npm: command not found`

**Fix:** Install Node.js from https://nodejs.org

---

### **Problem: Events don't show**
**Check:**
1. Backend is running (http://localhost:5000)
2. Events are imported (check database has data)
3. Frontend is connected to backend

**Test API:**
```bash
# Open browser and go to:
http://localhost:5000/api/events

# Should return JSON with events
```

---

### **Problem: Login fails**
**Fix:**
1. Make sure users are created (see Step 3)
2. Check backend console for errors
3. Try registering a new account

---

### **Problem: Recommendations are empty**
**Fix:**
1. Like at least 3-5 events first
2. Mark some as attended
3. Wait a few seconds for the GNN model to process
4. Refresh the page

---

## 🔧 **ADVANCED: Production Deployment**

### **Option 1: Local Network Access**

**Backend:**
```bash
python main.py
# Change in main.py: app.run(host="0.0.0.0", port=5000)
```

**Frontend:**
```bash
npm run dev -- --host
```

Access from other devices: `http://YOUR_IP:5173`

---

### **Option 2: Cloud Deployment**

**Backend (Heroku/Railway):**
1. Add `requirements.txt`:
```
flask
flask-cors
flask-jwt-extended
python-dotenv
sqlalchemy
pandas
scikit-learn
torch
torch-geometric
gunicorn
```

2. Add `Procfile`:
```
web: gunicorn main:app
```

3. Deploy to Heroku/Railway

**Frontend (Vercel/Netlify):**
```bash
npm run build
# Deploy 'dist' folder
```

---

### **Option 3: Docker** 🐳

**Backend Dockerfile:**
```dockerfile
FROM python:3.10
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "main.py"]
```

**Frontend Dockerfile:**
```dockerfile
FROM node:18
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build
CMD ["npm", "run", "preview"]
```

**docker-compose.yml:**
```yaml
version: '3'
services:
  backend:
    build: .
    ports:
      - "5000:5000"
  frontend:
    build: ./frontend
    ports:
      - "5173:5173"
```

Run: `docker-compose up`

---

## 🎊 **YOU'RE READY!**

Your EventConnect platform is now:
- ✅ Running locally
- ✅ Fully functional
- ✅ Ready for users
- ✅ Ready for demo
- ✅ Ready for production!

**Enjoy your social event recommendation platform!** 🚀

---

## 📞 **Support**

If you encounter issues:
1. Check the troubleshooting section above
2. Review console logs (both backend and frontend)
3. Check `PROJECT_COMPLETE.md` for architecture details

**Happy Launching!** 🎉