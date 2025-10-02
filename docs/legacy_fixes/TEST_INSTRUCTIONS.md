# 🧪 ФИНАЛНИ ТЕСТОВИ - Провери дали системот работи 100%

## ✅ ШТО ЌЕ ТЕСТИРАМЕ:

1. **Like копчето го менува rating-ot од 0 → 1**
2. **Dislike копчето го менува rating-от од 0 → -1**
3. **Скоровите се менуваат** кога се лајка/дислајка настан
4. **Toggle работи** (клик повторно → исчисти)

---

## 📝 ТЕСТ #1: Провери Like/Dislike копчиња

### **Чекор 1: Отвори страната**
1. Иди на: http://localhost:5173
2. Логирај се: **martin.stamenov03@gmail.com** / **test123**

### **Чекор 2: Отвори Browser Console**
- Chrome: `F12` → `Console` tab
- Firefox: `F12` → `Console` tab

### **Чекор 3: Тестирај Like копче**
1. Иди на **"Откријте"** таб
2. Најди некој настан (пр. "Седеф" или "Банда")
3. **Кликни на "❤️ Допаѓа ми се"**
4. **Провери во Console:**
   ```
   POST /api/events/XXX/rate HTTP/1.1 200
   {rating: 1}
   ```
5. **Провери визуелно:** Копчето треба да стане црвено/полнето
6. **Кликни повторно** на истото копче
7. **Провери:** Копчето треба да се исчисти (rating=0)

### **Чекор 4: Тестирај Dislike копче**
1. Најди друг настан
2. **Кликни на "👎 Не ми се допаѓа"**
3. **Провери во Console:**
   ```
   POST /api/events/XXX/rate HTTP/1.1 200
   {rating: -1}
   ```
4. **Провери визуелно:** Копчето треба да стане црвено
5. **Кликни повторно** на истото копче
6. **Провери:** Копчето треба да се исчисти

---

## 📝 ТЕСТ #2: Провери дали скоровите се менуваат

### **Scenário A: Лајкај IT events**

1. **Лајкај 5 IT events:**
   - "ИТ Настан: Хаклабот КИКА..."
   - "ИТ Настан: Сè Што Од Секогаш..."
   - "ИТ Настан: Брзината И Агилноста..."
   - "ИТ Настан: Launchub Ги Повикува..."
   - "ИТ Настан: Како Да Стартувате..."

2. **Иди на "Препораки" таб**

3. **Провери:**
   - IT events треба да имаат 95-100% ⭐⭐⭐⭐⭐
   - Non-IT events треба да имаат пониски скорови

### **Scenário B: Дислајкај Sports/Music events**

1. **Дислајкај 3-5 концертни настани:**
   - "Седеф"
   - "Банда"
   - "Атрактивната Лисица"
   - "Младост Среќа Иднина"
   - "Париз Те Сакам"

2. **Рефреш страната** (F5)

3. **Иди на "Препораки" таб**

4. **Провери:**
   - Концертни настани треба да **паднат** на 0-20% ❌
   - IT events треба да **останат** високо (95-100%) ⭐

---

## 📝 ТЕСТ #3: Провери Full 0-100% Range

### **Browser Console Test:**

1. Отвори **Console** (F12)
2. **Copy-paste** овој код:

```javascript
fetch('http://localhost:5000/api/recommend/me?limit=548', {
  headers: {'Authorization': 'Bearer ' + localStorage.getItem('token')}
})
.then(r => r.json())
.then(recs => {
  const scores = recs.map(r => r.score_pct);
  console.log('==== SCORING TEST RESULTS ====');
  console.log('Total events:', scores.length);
  console.log('Min score:', Math.min(...scores) + '%');
  console.log('Max score:', Math.max(...scores) + '%');
  console.log('Median score:', scores[Math.floor(scores.length/2)] + '%');

  // Покажи дистрибуција
  const ranges = {
    '90-100%': scores.filter(s => s >= 90).length,
    '70-89%': scores.filter(s => s >= 70 && s < 90).length,
    '50-69%': scores.filter(s => s >= 50 && s < 70).length,
    '30-49%': scores.filter(s => s >= 30 && s < 50).length,
    '0-29%': scores.filter(s => s < 30).length
  };
  console.log('Distribution:', ranges);

  console.log('\n✅ Test passed if:');
  console.log('- Total events = 548');
  console.log('- Min score close to 0%');
  console.log('- Max score = 100%');
  console.log('- Distribution is spread across all ranges');
});
```

### **Очекувани резултати:**
```
==== SCORING TEST RESULTS ====
Total events: 548
Min score: 0%
Max score: 100%
Median score: 50%

Distribution: {
  '90-100%': 55,
  '70-89%': 109,
  '50-69%': 109,
  '30-49%': 109,
  '0-29%': 166
}

✅ Test passed if:
- Total events = 548
- Min score close to 0%
- Max score = 100%
- Distribution is spread across all ranges
```

---

## 📝 ТЕСТ #4: Провери Toggle Behavior

### **Тест А: Like Toggle**
1. Лајкај настан → **копчето полнето црвено** ✅
2. Кликни повторно → **копчето се исчистува** ✅
3. Кликни трет пат → **копчето пак полнето** ✅

### **Тест Б: Dislike Toggle**
1. Дислајкај настан → **копчето полнето црвено** ✅
2. Кликни повторно → **копчето се исчистува** ✅
3. Кликни трет пат → **копчето пак полнето** ✅

### **Тест В: Like + Dislike Mutual Exclusion**
1. Лајкај настан → **like копче е активно**
2. Кликни на **dislike** → **like се исчистува, dislike активно** ✅
3. Кликни на **like** → **dislike се исчистува, like активно** ✅

---

## 🎯 РЕЗУЛТАТИ ШТО ТРЕБА ДА ГИ ВИДИШ:

### **✅ Успешен Тест:**
- POST /api/events/XXX/rate враќа **200 OK**
- Копчињата се **полнат/празнат** правилно
- Скоровите се **менуваат** по like/dislike
- IT events со likes → **95-100%** ⭐⭐⭐⭐⭐
- Music events со dislikes → **0-20%** ❌
- Full range: **0% до 100%** (не 81-100%)

### **❌ Ако нешто не работи:**
1. Провери дали backend е активен: http://localhost:5000/api/health
2. Провери во Network tab дали има 404/500 грешки
3. Провери дали токенот е валиден: `localStorage.getItem('token')`
4. Рефреш страната (F5) и пробај повторно

---

## 🎉 АКО СИТЕ ТЕСТОВИ ПОМИНУВААТ:

**ЧЕСТИТКИ! СИСТЕМОТ Е 100% ФУНКЦИОНАЛЕН!** 🚀

Сега имаш:
- ✅ Функционални Like/Dislike копчиња
- ✅ Rating систем (-1, 0, 1)
- ✅ Скорови од 0-100% врз основа на user preferences
- ✅ GNN препорачувач кој работи совршено
- ✅ Врвна платформа за event препораки!

---

## 📞 ДА ПОЧНЕШ СО ТЕСТИРАЊЕ:

1. **Отвори**: http://localhost:5173
2. **Логирај се**: martin.stamenov03@gmail.com / test123
3. **Следи ги чекорите** погоре
4. **Ужива** во твојата врвна платформа! 🎯

---

**Сè е спремно! Сега само тестирај и види ја магијата!** ✨