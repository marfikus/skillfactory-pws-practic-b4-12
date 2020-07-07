
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

def find_nearby_athletes(session, user):
    # Флаги для дальнейшего управления алгоритмом поиска:
    athlete_nearby_height_is_found = False
    athlete_nearby_birthdate_is_found = False

    # Ищем точное совпадение роста:
    athlete = session.query(Athelete).filter(Athelete.height == user.height).first()
    if not athlete is None:
        print(athlete.name)
        athlete_nearby_height_is_found = True
        
    # Ищем точное совпадение даты рождения:
    athlete = session.query(Athelete).filter(Athelete.birthdate == user.birthdate).first() # !!! изменить формат записи при добавлении юзера
    if not athlete is None:
        print(athlete.name)
        athlete_nearby_birthdate_is_found = True
        
    # Если не нашли, то будем перебирать все записи и искать ближайшее значение.
    # Способ конечно стрёмный (для больших объёмов), но в общем рабочий 
    if not athlete_nearby_height_is_found or not athlete_nearby_birthdate_is_found:
        athletes = session.query(Athelete).all()
        # Устанавливаем начальное минимальное значение разницы 
        # между ростом юзера и первого атлета.
        # Пока это просто рост юзера
        min_dif_heights = user.height
        athlete_nearby_height = None
        
        # Аналогично поступаем и с датой рождения, 
        # предварительно преобразовав её в удобный для манипуляций формат
        user_birthdate = dt.datetime.strptime(user.birthdate, "%Y-%m-%d")
        min_dif_birthdates = dt.timedelta.max
        athlete_nearby_birthdate = None
            
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
                        
        if not athlete_nearby_height is None:
            print("min_dif_heights:", min_dif_heights)
            print("athlete_nearby_height:", athlete_nearby_height.name)
    
        if not athlete_nearby_birthdate is None:
            print("min_dif_birthdates:", min_dif_birthdates)
            print("athlete_nearby_birthdate:", athlete_nearby_birthdate.name)

def main():
    session = connect_db()

    user_id = input("Enter user id: ")

    user = session.query(User).filter(User.id == user_id).first()
    if user is None:
        print("No user with id:", user_id)
        return

    find_nearby_athletes(session, user)

if __name__ == "__main__":
    main()
