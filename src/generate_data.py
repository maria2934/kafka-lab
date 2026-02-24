# src/generate_data.py

import random
from datetime import datetime
import json
import time

class SalesDataGenerator:
    def __init__(self):
        self.products = [
            "Laptop", "Mouse", "Keyboard", "Monitor", "Headphones",
            "Webcam", "Router", "Tablet", "Smartphone", "Printer"
        ]
        self.regions = ["North", "South", "East", "West", "Central"]
        self.order_id = 10001

    def generate_sale(self):
        sale = {
            "order_id": self.order_id,
            "product": random.choice(self.products),
            "price": round(random.uniform(20, 1500), 2),
            "quantity": random.randint(1, 5),
            "region": random.choice(self.regions),
            "timestamp": datetime.now().isoformat()
        }
        self.order_id += 1
        return sale

    def stream(self, interval=1):
        """Генерирует одну продажу в секунду"""
        while True:
            yield self.generate_sale()
            time.sleep(interval)
