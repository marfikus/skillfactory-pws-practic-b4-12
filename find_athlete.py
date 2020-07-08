
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import datetime as dt

DB_PATH = "sqlite:///sochi_athletes.sqlite3"

Base = declarative_base()

class User(Base):
    __tablename__ = "user"
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    first_name = sa.Column(sa.Text)
    last_name = sa.Column(sa.Text)
    gender = sa.Column(sa.Text)
    email = sa.Column(sa.Text)
    birthdate = sa.Column(sa.Text)
    height = sa.Column(sa.Float)

class Athelete(Base):
    __tablename__ = "athelete"
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    age = sa.Column(sa.Integer)
    birthdate = sa.Column(sa.Text)
    gender = sa.Column(sa.Text)
    height = sa.Column(sa.Float)
    name = sa.Column(sa.Text)
    weight = sa.Column(sa.Integer)
    gold_medals = sa.Column(sa.Integer)
    silver_medals = sa.Column(sa.Integer)
    bronze_medals = sa.Column(sa.Integer)
    total_medals = sa.Column(sa.Integer)
    sport = sa.Column(sa.Text)
    country = sa.Column(sa.Text)

def connect_db():
    # создаём подключение к бд
    engine = sa.create_engine(DB_PATH)
    # создаём описанные таблицы
    Base.metadata.create_all(engine)
    # создаём фабрику сессий
    Sessions = sessionmaker(engine)
    # создаём сессию
    session = Sessions()
    return session

def print_user(user):
    output = (
        f" Id: {user.id}\n"
        f" Name: {user.first_name} {user.last_name}\n"
        f" Gender: {user.gender}\n"
        f" Email: {user.email}\n" 
        f" Birthdate: {user.birthdate}\n"
        f" Height: {user.height}\n"
    )
    print(output)
    
def print_athlete(athlete):
    output = (
        f" Id: {athlete.id}\n"
        f" Name: {athlete.name}\n"
        f" Age: {athlete.age}\n"
        f" Birthdate: {athlete.birthdate}\n"
        f" Gender: {athlete.gender}\n"
        f" Height: {athlete.height}\n"
        f" Weight: {athlete.weight}\n"
        f" Gold_medals: {athlete.gold_medals}\n"
        f" Silver_medals: {athlete.silver_medals}\n"
        f" Bronze_medals: {athlete.bronze_medals}\n"
        f" Total_medals: {athlete.total_medals}\n"
        f" Sport: {athlete.sport}\n"
        f" Country: {athlete.country}\n"
    ) 
    print(output)
    
def find_nearby_athletes(session, user):
    # Флаги для дальнейшего управления алгоритмом поиска:
    athlete_nearby_height_is_found = False
    athlete_nearby_birthdate_is_found = False

    # Наименьшая разница в росте юзера и атлета
    # Будет нужна при поиске в цикле, а пока устанавливаем в 0
    min_dif_heights = 0

    # Наименьшая разница в датах рождения юзера и атлета
    # Также для поиска, а пока в 0
    min_dif_birthdates = 0

    # Ищем точное совпадение роста:
    athlete_nearby_height = session.query(Athelete).filter(Athelete.height == user.height).first()
    # Если нашли такого атлета:
    if not athlete_nearby_height is None:
        # Отмечаем это
        athlete_nearby_height_is_found = True
        
    # Ищем точное совпадение даты рождения:
    athlete_nearby_birthdate = session.query(Athelete).filter(Athelete.birthdate == user.birthdate).first()
    # Если нашли такого атлета:
    if not athlete_nearby_birthdate is None:
        # Отмечаем это
        athlete_nearby_birthdate_is_found = True
        
    # Если не нашли атлета по росту или по дате рождения, 
    # то будем перебирать все записи и искать ближайшее значение.
    # Способ конечно стрёмный (для больших объёмов), но в общем рабочий 
    if not athlete_nearby_height_is_found or not athlete_nearby_birthdate_is_found:
        athletes = session.query(Athelete).all()

        # Если не нашли атлета по росту
        if not athlete_nearby_height_is_found:
            # Устанавливаем наименьшей разницей рост юзера
            # Это только для начала поиска, а далее оно будет вычисляться.
            # Причём значение должно быть гарантированно больше того, 
            # которое будет вычислено далее 
            # (разумеется, при соблюдении одного масштаба в росте :) )
            min_dif_heights = user.height
        
        # Если не нашли атлета по дате рождения
        if not athlete_nearby_birthdate_is_found:
            # Устанавливаем наименьшей разницей максимальное значение промежутка времени.
            # Это тоже только для начала поиска, потом пересчитается...
            min_dif_birthdates = dt.timedelta.max
            # Преобразуем дату рождения юзера в удобный для манипуляций формат
            user_birthdate = dt.datetime.strptime(user.birthdate, "%Y-%m-%d")
            
        for athlete in athletes:
            if not athlete_nearby_height_is_found:
                # У некоторых атлетов не указан рост, пропускаем их.
                if not athlete.height is None:
                    # Считаем разницу (по модулю) между ростом юзера и атлета.
                    dif_heights = abs(user.height - athlete.height)
                    # Если она меньше текущей минимальной,
                    # то переписываем её и атлета у которого она обнаружена:
                    if dif_heights < min_dif_heights:
                        min_dif_heights = dif_heights
                        athlete_nearby_height = athlete

            if not athlete_nearby_birthdate_is_found:
                #  На всякий случай и наличие даты рождения у атлета проверяем:
                if not athlete.birthdate is None:
                    # Приводим дату рождения атлета к удобному для манипуляций виду:
                    athlete_birthdate = dt.datetime.strptime(athlete.birthdate, "%Y-%m-%d")
                    # Аналогично считаем разницу (по модулю) в датах рождения.
                    # Если она меньше текущей минимальной,
                    # то переписываем её и атлета у которого она обнаружена:
                    dif_birthdates = abs(user_birthdate - athlete_birthdate)
                    if dif_birthdates < min_dif_birthdates:
                        min_dif_birthdates = dif_birthdates
                        athlete_nearby_birthdate = athlete

    result = {
        "athlete_nearby_height": athlete_nearby_height,
        "dif_heights": min_dif_heights,
        "athlete_nearby_birthdate": athlete_nearby_birthdate,
        "dif_birthdates": min_dif_birthdates
    }
    return result

def main():
    session = connect_db()

    # Запрашиваем ид юзера, ищем его в базе
    user_id = input("Enter user id: ")
    user = session.query(User).filter(User.id == user_id).first()
    # Если нет такого, сообщаем и выходим
    if user is None:
        print("No user with id:", user_id)
        return
    
    # Иначе выводим данные юзера
    print("\nSelected user:")
    print_user(user)
    
    # Ищем в базе ближайших атлетов
    result = find_nearby_athletes(session, user)
    # Если найден ближайший по росту, то выводим его данные
    if not result["athlete_nearby_height"] is None:
        print("Nearest athlete by height:")
        print_athlete(result["athlete_nearby_height"])
        print(f"Heights difference: {result['dif_heights']}\n")
    else:
        print("No nearest athlete by height\n")
    # Если найден ближайший по дате рождения то выводим его данные
    if not result["athlete_nearby_birthdate"] is None:
        print("Nearest athlete by birthdate:")
        print_athlete(result["athlete_nearby_birthdate"])
        print(f"Birthdates difference: {result['dif_birthdates']}\n")
    else:
        print("No nearest athlete by birthdate\n")

if __name__ == "__main__":
    main()
