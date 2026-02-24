import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report
import joblib
import os

# Проверяем, есть ли данные
if not os.path.exists('received_sales.csv'):
    print("❌ Нет данных. Сначала запусти consumer.py")
    exit()

# Загружаем полученные данные
df = pd.read_csv('received_sales.csv')

# Целевая переменная: высокая цена (>500)
df['high_value'] = (df['price'] > 500).astype(int)

# Кодируем категориальные признаки
le_cat = LabelEncoder()
le_reg = LabelEncoder()
df['category_enc'] = le_cat.fit_transform(df['category'])
df['region_enc'] = le_reg.fit_transform(df['region'])

# Фичи
X = df[['category_enc', 'region_enc', 'quantity']]
y = df['high_value']

# Обучение
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
model = RandomForestClassifier(n_estimators=50, random_state=42)
model.fit(X_train, y_train)

# Оценка
preds = model.predict(X_test)
print("\\n📊 Отчёт по классификации:")
print(classification_report(y_test, preds))

# Сохраним модель
joblib.dump(model, 'high_value_model.pkl')
joblib.dump(le_cat, 'label_encoder_category.pkl')
joblib.dump(le_reg, 'label_encoder_region.pkl')
print("✅ Модель и кодировщики сохранены")
