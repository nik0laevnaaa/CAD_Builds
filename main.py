import pyodbc
from datetime import datetime, timedelta
import random

# --------- НАСТРОЙКИ ---------
SERVER_NAME = r"(local)"   # здесь при необходимости замени на свое имя сервера
DATABASE_NAME = "CAD_Builds_DW"
FACT_ROWS = 12000               # >= 10 000
USERS_COUNT = 50
BUILDS_COUNT = 300
YEARS_BACK = 5                  # данные за последние несколько лет

# --------- ПОДКЛЮЧЕНИЕ К СЕРВЕРУ (master) ---------
conn_master = pyodbc.connect(
    fr"DRIVER={{SQL Server}};SERVER={SERVER_NAME};DATABASE=master;Trusted_Connection=yes;",
    autocommit=True
)
cur_master = conn_master.cursor()

# --------- СОЗДАНИЕ БАЗЫ ДАННЫХ ---------
cur_master.execute(f"""
IF DB_ID('{DATABASE_NAME}') IS NOT NULL
    DROP DATABASE [{DATABASE_NAME}];
CREATE DATABASE [{DATABASE_NAME}];
""")
print("База данных создана.")

conn_master.close()

# --------- ПОДКЛЮЧЕНИЕ К НОВОЙ БД ---------
conn = pyodbc.connect(
    fr"DRIVER={{SQL Server}};SERVER={SERVER_NAME};DATABASE={DATABASE_NAME};Trusted_Connection=yes;",
    autocommit=True
)
cur = conn.cursor()

# --------- СОЗДАНИЕ ТАБЛИЦ ---------
cur.execute("""
IF OBJECT_ID('Fact_ChangeHistory') IS NOT NULL DROP TABLE Fact_ChangeHistory;
IF OBJECT_ID('Dim_Builds') IS NOT NULL DROP TABLE Dim_Builds;
IF OBJECT_ID('Dim_Users')  IS NOT NULL DROP TABLE Dim_Users;
IF OBJECT_ID('Dim_UserRole')  IS NOT NULL DROP TABLE Dim_UserRole;
IF OBJECT_ID('Dim_Status')  IS NOT NULL DROP TABLE Dim_Status;
IF OBJECT_ID('Dim_Type')  IS NOT NULL DROP TABLE Dim_Type;
""")

cur.execute("""
CREATE TABLE Dim_Users (
    User_ID          INT        NOT NULL PRIMARY KEY,
    Login            VARCHAR(50)  NOT NULL,
    Full_Name        VARCHAR(100) NOT NULL,
    Registration_Date DATETIME    NOT NULL
);
""")

cur.execute("""
CREATE TABLE Dim_UserRole (
    User_ID   INT         NOT NULL PRIMARY KEY,
    Role_Name VARCHAR(50) NOT NULL,
    CONSTRAINT FK_UserRole_User
        FOREIGN KEY (User_ID) REFERENCES Dim_Users(User_ID)
);
""")

cur.execute("""
CREATE TABLE Dim_Builds (
    Build_ID      INT         NOT NULL PRIMARY KEY,
    Version       VARCHAR(50)  NOT NULL,
    Link          VARCHAR(100) NOT NULL,
    Creation_Date DATETIME     NOT NULL,
    Changelog     TEXT         NULL
);
""")

cur.execute("""
CREATE TABLE Dim_Status (
    Build_ID    INT         NOT NULL PRIMARY KEY,
    Status_Name VARCHAR(50) NOT NULL,
    CONSTRAINT FK_Status_Build
        FOREIGN KEY (Build_ID) REFERENCES Dim_Builds(Build_ID)
);
""")

cur.execute("""
CREATE TABLE Dim_Type (
    Build_ID  INT         NOT NULL PRIMARY KEY,
    Type_Name VARCHAR(50) NOT NULL,
    CONSTRAINT FK_Type_Build
        FOREIGN KEY (Build_ID) REFERENCES Dim_Builds(Build_ID)
);
""")

cur.execute("""
CREATE TABLE Fact_ChangeHistory (
    Change_ID   INT         NOT NULL PRIMARY KEY,
    Ex_Status   VARCHAR(50) NOT NULL,
    New_Status  VARCHAR(50) NOT NULL,
    Change_Date DATETIME    NOT NULL,
    User_ID     INT         NOT NULL,
    Build_ID    INT         NOT NULL,
    CONSTRAINT FK_Fact_User  FOREIGN KEY (User_ID)  REFERENCES Dim_Users(User_ID),
    CONSTRAINT FK_Fact_Build FOREIGN KEY (Build_ID) REFERENCES Dim_Builds(Build_ID)
);
""")
print("Таблицы созданы.")

# --------- ПАРАМЕТРЫ ГЕНЕРАЦИИ ---------
USERS_COUNT  = 50          # 10–100 записей для пользователей
BUILDS_COUNT = 300         # 10–100+ записей для сборок
FACT_ROWS    = 12000       # >= 10 000 строк в факте
YEARS_BACK   = 5           # данные за последние несколько лет

now = datetime.now()
roles    = ["Developer", "Tester", "Admin", "Manager"]
statuses = ["Testing", "Release", "Rejected"]
types    = ["dev", "beta", "release"]

# --------- ФУНКЦИЯ СЛУЧАЙНОЙ ДАТЫ ЗА N ЛЕТ НАЗАД ---------
def random_date_within_years(years_back: int) -> datetime:
    days_back = random.randint(0, years_back * 365)
    seconds   = random.randint(0, 24 * 3600 - 1)
    return now - timedelta(days=days_back, seconds=seconds)

# ================== ЗАПОЛНЕНИЕ ИЗМЕРЕНИЙ ==================

# 1. Dim_Users
for uid in range(1, USERS_COUNT + 1):
    login = f"user{uid}"
    full_name = f"User {uid}"
    reg_date = random_date_within_years(YEARS_BACK)
    cur.execute("""
        INSERT INTO Dim_Users (User_ID, Login, Full_Name, Registration_Date)
        VALUES (?, ?, ?, ?)
    """, uid, login, full_name, reg_date)

print(f"В Dim_Users вставлено {USERS_COUNT} строк.")

# 2. Dim_UserRole (одна роль на пользователя)
for uid in range(1, USERS_COUNT + 1):
    role = random.choice(roles)
    cur.execute("""
        INSERT INTO Dim_UserRole (User_ID, Role_Name)
        VALUES (?, ?)
    """, uid, role)

print(f"В Dim_UserRole вставлено {USERS_COUNT} строк.")

# 3. Dim_Builds
for bid in range(1, BUILDS_COUNT + 1):
    version = f"v{1 + bid % 5}.{bid % 10}.{bid % 20}"
    link = f"https://repo.example.com/builds/{bid}"
    creation_date = random_date_within_years(YEARS_BACK)
    changelog = f"Auto-generated changelog for build {bid}"
    cur.execute("""
        INSERT INTO Dim_Builds (Build_ID, Version, Link, Creation_Date, Changelog)
        VALUES (?, ?, ?, ?, ?)
    """, bid, version, link, creation_date, changelog)

print(f"В Dim_Builds вставлено {BUILDS_COUNT} строк.")

# 4. Dim_Status (по одному статусу на сборку)
for bid in range(1, BUILDS_COUNT + 1):
    status_name = random.choice(statuses)
    cur.execute("""
        INSERT INTO Dim_Status (Build_ID, Status_Name)
        VALUES (?, ?)
    """, bid, status_name)

print(f"В Dim_Status вставлено {BUILDS_COUNT} строк.")

# 5. Dim_Type (по одному типу на сборку)
for bid in range(1, BUILDS_COUNT + 1):
    type_name = random.choice(types)
    cur.execute("""
        INSERT INTO Dim_Type (Build_ID, Type_Name)
        VALUES (?, ?)
    """, bid, type_name)

print(f"В Dim_Type вставлено {BUILDS_COUNT} строк.")

# ================== ЗАПОЛНЕНИЕ ТАБЛИЦЫ ФАКТОВ ==================

for cid in range(1, FACT_ROWS + 1):
    ex_status  = random.choice(statuses)
    new_status = random.choice(statuses)
    change_date = random_date_within_years(YEARS_BACK)
    user_id  = random.randint(1, USERS_COUNT)
    build_id = random.randint(1, BUILDS_COUNT)

    cur.execute("""
        INSERT INTO Fact_ChangeHistory
            (Change_ID, Ex_Status, New_Status, Change_Date, User_ID, Build_ID)
        VALUES (?, ?, ?, ?, ?, ?)
    """, cid, ex_status, new_status, change_date, user_id, build_id)

print(f"В Fact_ChangeHistory вставлено {FACT_ROWS} строк.")
