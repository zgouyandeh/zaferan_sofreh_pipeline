"""
generators/reference_data.py
-----------------------------
This module generates synthetic reference data for the Zaferan Sofreh restaurant analytics pipeline.
It creates restaurant branches, menu items, and customers, and writes the output to CSV files for use in the bronze
layer of the pipeline.
Tables produced:
    restaurants.csv  - one row per branch
    menu_items.csv   - branch-level menu (master menu x branch, with a
                        small per-branch price variance to mimic real
                        pricing differences across locations)
    customers.csv     - synthetic customer base
"""
from __future__ import annotations

import random
import pandas as pd
from config import CONFIG
from logging_setup import get_logger

logger = get_logger(__name__)
random.seed(CONFIG.random_seed)

RESTAURANTS_MASTER = [
    {"restaurant_id": "ZAF-THR-001", "name": "Zaferan Sofreh Valiasr", "city": "Tehran", "province": "Tehran", "country": "Iran", "address": "Valiasr Street, Tehran", "opening_date": "2023-01-15", "phone": "+98-21-8812-4567"},
    {"restaurant_id": "ZAF-THR-002", "name": "Zaferan Sofreh Tajrish", "city": "Tehran", "province": "Tehran", "country": "Iran", "address": "Tajrish Square, Tehran", "opening_date": "2023-06-20", "phone": "+98-21-2274-5678"},
    {"restaurant_id": "ZAF-ISF-001", "name": "Zaferan Sofreh Naqsh-e Jahan", "city": "Isfahan", "province": "Isfahan", "country": "Iran", "address": "Naqsh-e Jahan Square, Isfahan", "opening_date": "2023-03-10", "phone": "+98-31-3222-6789"},
    {"restaurant_id": "ZAF-SHZ-001", "name": "Zaferan Sofreh Zandiyeh", "city": "Shiraz", "province": "Fars", "country": "Iran", "address": "Zand Boulevard, Shiraz", "opening_date": "2023-09-05", "phone": "+98-71-3233-7890"},
    {"restaurant_id": "ZAF-TBZ-001", "name": "Zaferan Sofreh El-Goli", "city": "Tabriz", "province": "East Azerbaijan", "country": "Iran", "address": "El-Goli Park Road, Tabriz", "opening_date": "2024-02-14", "phone": "+98-41-3355-8901"},
    {"restaurant_id": "ZAF-MSH-001", "name": "Zaferan Sofreh Vakilabad", "city": "Mashhad", "province": "Razavi Khorasan", "country": "Iran", "address": "Vakilabad Boulevard, Mashhad", "opening_date": "2024-05-01", "phone": "+98-51-3811-9012"},
]

MASTER_MENU = [
    # --- Brunch (10:00 AM - 11:00 AM) -----------------------------------
    {"item_id": "ITEM-001", "name": "Halim Gandom", "category": "Brunch", "price": 110000, "ingredients": "Wheat, Meat, Cinnamon, Sugar", "is_vegetarian": False, "spice_level": "None"},
    {"item_id": "ITEM-002", "name": "Kalleh Pacheh Portion", "category": "Brunch", "price": 180000, "ingredients": "Lamb Trotters & Head, Cinnamon, Lemon", "is_vegetarian": False, "spice_level": "None"},
    {"item_id": "ITEM-003", "name": "Nane Panir Sabzi & Khameh", "category": "Brunch", "price": 85000, "ingredients": "Flatbread, Feta Cheese, Fresh Herbs, Clotted Cream, Honey", "is_vegetarian": True, "spice_level": "None"},
    {"item_id": "ITEM-004", "name": "Omelette Irani", "category": "Brunch", "price": 75000, "ingredients": "Eggs, Tomato Paste, Fresh Sangak Bread", "is_vegetarian": True, "spice_level": "Mild"},

    # --- Starters -------------------------------------------------------
    {"item_id": "ITEM-101", "name": "Mast-o Khiar", "category": "Starter", "price": 65000, "ingredients": "Yogurt, Cucumber, Mint, Dried Rose Petals", "is_vegetarian": True, "spice_level": "None"},
    {"item_id": "ITEM-102", "name": "Kashke Bademjan", "category": "Starter", "price": 95000, "ingredients": "Eggplant, Whey (Kashk), Fried Onion, Mint", "is_vegetarian": True, "spice_level": "Mild"},
    {"item_id": "ITEM-103", "name": "Mirza Ghasemi", "category": "Starter", "price": 90000, "ingredients": "Smoked Eggplant, Tomato, Garlic, Egg", "is_vegetarian": True, "spice_level": "Mild"},
    {"item_id": "ITEM-104", "name": "Dolme Barg", "category": "Starter", "price": 110000, "ingredients": "Grape Leaves, Rice, Herbs, Ground Meat", "is_vegetarian": False, "spice_level": "Mild"},
    {"item_id": "ITEM-105", "name": "Salad Shirazi", "category": "Starter", "price": 55000, "ingredients": "Cucumber, Tomato, Onion, Lime, Mint", "is_vegetarian": True, "spice_level": "None"},

    # --- Main Course - Lunch / Dinner ------------------------------------
    {"item_id": "ITEM-201", "name": "Kuku Sabzi", "category": "Main Course", "price": 120000, "ingredients": "Herbs, Egg, Walnuts, Barberries", "is_vegetarian": True, "spice_level": "None"},
    {"item_id": "ITEM-202", "name": "Adas Polo", "category": "Main Course", "price": 130000, "ingredients": "Rice, Lentils, Raisins, Dates, Fried Onion", "is_vegetarian": True, "spice_level": "Mild"},
    {"item_id": "ITEM-203", "name": "Ash Reshteh", "category": "Main Course", "price": 105000, "ingredients": "Noodles, Herbs, Beans, Kashk", "is_vegetarian": True, "spice_level": "Mild"},
    {"item_id": "ITEM-204", "name": "Sabzi Polo", "category": "Main Course", "price": 95000, "ingredients": "Herbed Basmati Rice, Dill, Parsley", "is_vegetarian": True, "spice_level": "None"},
    {"item_id": "ITEM-205", "name": "Baghali Polo", "category": "Main Course", "price": 115000, "ingredients": "Rice, Fava Beans, Dill", "is_vegetarian": True, "spice_level": "None"},
    {"item_id": "ITEM-301", "name": "Chelo Kabab Koobideh", "category": "Main Course", "price": 285000, "ingredients": "Ground Lamb & Beef Skewers, Saffron Rice, Grilled Tomato", "is_vegetarian": False, "spice_level": "Mild"},
    {"item_id": "ITEM-302", "name": "Joojeh Kabab", "category": "Main Course", "price": 260000, "ingredients": "Saffron Marinated Chicken, Grilled Tomato, Rice", "is_vegetarian": False, "spice_level": "Mild"},
    {"item_id": "ITEM-303", "name": "Ghormeh Sabzi", "category": "Main Course", "price": 245000, "ingredients": "Lamb, Herbs, Dried Lime, Kidney Beans", "is_vegetarian": False, "spice_level": "Medium"},
    {"item_id": "ITEM-304", "name": "Fesenjan", "category": "Main Course", "price": 270000, "ingredients": "Chicken, Walnut, Pomegranate Molasses", "is_vegetarian": False, "spice_level": "Mild"},
    {"item_id": "ITEM-305", "name": "Zereshk Polo ba Morgh", "category": "Main Course", "price": 230000, "ingredients": "Saffron Rice, Barberries, Roast Chicken", "is_vegetarian": False, "spice_level": "None"},
    {"item_id": "ITEM-306", "name": "Tahchin Morgh", "category": "Main Course", "price": 220000, "ingredients": "Saffron Rice Cake, Chicken, Yogurt", "is_vegetarian": False, "spice_level": "None"},
    {"item_id": "ITEM-307", "name": "Abgoosht (Dizi)", "category": "Main Course", "price": 195000, "ingredients": "Lamb, Chickpeas, Potato, Dried Lime", "is_vegetarian": False, "spice_level": "Mild"},

    # --- Breads ------------------------------------------------------------
    {"item_id": "ITEM-401", "name": "Sangak Bread", "category": "Bread", "price": 25000, "ingredients": "Whole Wheat Flour, Sesame", "is_vegetarian": True, "spice_level": "None"},
    {"item_id": "ITEM-402", "name": "Barbari Bread", "category": "Bread", "price": 22000, "ingredients": "Flour, Nigella Seeds", "is_vegetarian": True, "spice_level": "None"},
    {"item_id": "ITEM-403", "name": "Lavash Bread", "category": "Bread", "price": 15000, "ingredients": "Flour, Water, Salt", "is_vegetarian": True, "spice_level": "None"},

    # --- Desserts ------------------------------------------------------------
    {"item_id": "ITEM-501", "name": "Bastani Sonnati", "category": "Dessert", "price": 75000, "ingredients": "Saffron, Rosewater, Pistachio, Cream", "is_vegetarian": True, "spice_level": "None"},
    {"item_id": "ITEM-502", "name": "Faloodeh Shirazi", "category": "Dessert", "price": 65000, "ingredients": "Frozen Vermicelli, Rosewater, Lime, Cherry Syrup", "is_vegetarian": True, "spice_level": "None"},
    {"item_id": "ITEM-503", "name": "Sholeh Zard", "category": "Dessert", "price": 55000, "ingredients": "Rice, Saffron, Cinnamon, Rosewater", "is_vegetarian": True, "spice_level": "None"},
    {"item_id": "ITEM-504", "name": "Zoolbia Bamieh", "category": "Dessert", "price": 60000, "ingredients": "Fried Batter, Saffron Syrup", "is_vegetarian": True, "spice_level": "None"},

    # --- Beverages ------------------------------------------------------------
    {"item_id": "ITEM-601", "name": "Doogh", "category": "Beverage", "price": 35000, "ingredients": "Yogurt, Mint, Soda Water", "is_vegetarian": True, "spice_level": "None"},
    {"item_id": "ITEM-602", "name": "Sharbat-e Sekanjebin", "category": "Beverage", "price": 40000, "ingredients": "Vinegar, Sugar, Mint, Cucumber", "is_vegetarian": True, "spice_level": "None"},
    {"item_id": "ITEM-603", "name": "Persian Tea (Chai)", "category": "Beverage", "price": 25000, "ingredients": "Black Tea, Cardamom, Saffron Rock Candy", "is_vegetarian": True, "spice_level": "None"},
    {"item_id": "ITEM-604", "name": "Sharbat-e Ablimoo", "category": "Beverage", "price": 38000, "ingredients": "Lime Juice, Mint, Sugar", "is_vegetarian": True, "spice_level": "None"},
]

IRANIAN_FIRST_NAMES_MALE = ["Ali", "Reza", "Mohammad", "Hossein", "Amir", "Arash", "Kaveh", "Babak", "Farhad", "Kian", "Saeed", "Navid", "Peyman", "Shahram", "Bahram", "Sina", "Pouya", "Ehsan", "Iman", "Meysam"]
IRANIAN_FIRST_NAMES_FEMALE = ["Sara", "Maryam", "Niloofar", "Yasaman", "Parisa", "Shirin", "Leila", "Roya", "Fatemeh", "Zahra", "Mahsa", "Nazanin", "Elham", "Bahar", "Golnaz", "Setareh", "Azadeh", "Tara", "Donya", "Sepideh"]
IRANIAN_LAST_NAMES = ["Hosseini", "Mohammadi", "Rezaei", "Karimi", "Ahmadi", "Moradi", "Sadeghi", "Ebrahimi", "Ghorbani", "Rahimi", "Jafari", "Salehi", "Kazemi", "Fallahi", "Zare", "Nasseri", "Farahani", "Amini", "Bahrami", "Tehrani", "Isfahani", "Shirazi", "Tabrizi"]
IRANIAN_CITIES = ["Tehran", "Isfahan", "Shiraz", "Mashhad", "Tabriz", "Karaj"]

def _random_iranian_name() -> str:
    first = random.choice(IRANIAN_FIRST_NAMES_MALE) if random.random() < 0.5 else random.choice(IRANIAN_FIRST_NAMES_FEMALE)
    return f"{first} {random.choice(IRANIAN_LAST_NAMES)}"

def _random_iranian_phone() -> str:
    return f"+98-9{random.randint(10, 39)}-{random.randint(100,999)}-{random.randint(1000,9999)}"

def _random_email(name: str) -> str:
    return f"{name.lower().replace(' ', '.')}{random.randint(1, 999)}@{random.choice(['gmail.com', 'yahoo.com', 'outlook.com'])}"

def generate_restaurants() -> pd.DataFrame:
    logger.info("Generating %d restaurant branches", len(RESTAURANTS_MASTER))
    return pd.DataFrame(RESTAURANTS_MASTER)

def generate_menu_items() -> pd.DataFrame:
    restaurants = generate_restaurants().to_dict("records")
    menu_rows = []
    for restaurant in restaurants:
        rest_id = restaurant["restaurant_id"]
        for item in MASTER_MENU:
            price_multiplier = random.uniform(0.95, 1.05)
            menu_rows.append({
                "restaurant_id": rest_id,
                "item_id": item["item_id"],
                "name": item["name"],
                "category": item["category"],
                "price": round(item["price"] * price_multiplier / 1000) * 1000,
                "ingredients": item["ingredients"],
                "is_vegetarian": item["is_vegetarian"],
                "spice_level": item["spice_level"],
            })
    logger.info("Generated %d menu rows (%d branches x %d master items)", len(menu_rows), len(restaurants), len(MASTER_MENU))
    return pd.DataFrame(menu_rows)

def generate_customers(n: int) -> pd.DataFrame:
    from datetime import date, timedelta
    customers = []
    today = date.today()
    for i in range(n):
        name = _random_iranian_name()
        customers.append({
            "customer_id": f"CUST-{10000 + i}",
            "name": name,
            "email": _random_email(name),
            "phone": _random_iranian_phone(),
            "city": random.choice(IRANIAN_CITIES),
            "join_date": (today - timedelta(days=random.randint(0, 2 * 365))).strftime("%Y-%m-%d"),
        })
    logger.info("Generated %d customers", n)
    return pd.DataFrame(customers)

def generate_data_for_sql_db() -> None:
    CONFIG.ensure_dirs()
    df_restaurants = generate_restaurants()
    df_menu_items = generate_menu_items()
    df_customers = generate_customers(CONFIG.n_customers)

    df_restaurants.to_csv(CONFIG.restaurants_path, index=False)
    df_menu_items.to_csv(CONFIG.menu_items_path, index=False)
    df_customers.to_csv(CONFIG.customers_path, index=False)

    logger.info("Wrote restaurants.csv    -> %d rows", len(df_restaurants))
    logger.info("Wrote menu_items.csv     -> %d rows", len(df_menu_items))
    logger.info("Wrote customers.csv      -> %d rows", len(df_customers))

if __name__ == "__main__":
    generate_data_for_sql_db()