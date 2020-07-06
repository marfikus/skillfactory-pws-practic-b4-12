
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

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

def find_athlete_nearby_height(session, user):
    # Флаг для дальнейшего прерывания алгоритма поиска:
    # athlete_nearby_height_is_found = False

    # Ищем точное совпадение роста:
    athlete = session.query(Athelete).filter(Athelete.height == user.height).first()
    if not athlete is None:
        print(athlete.name)
        # athlete_nearby_height_is_found = True
        return

    # Если не нашли, то будем перебирать все записи и искать ближайшее значение.
    # Способ конечно стрёмный (для больших объёмов), но в общем рабочий
    athletes = session.query(Athelete).all()
    # print(len(athletes))
    # print(athletes[0].height)
    print("user.height:", user.height)
    # Считаем разницу (по модулю) между ростом юзера и первого атлета.
    # Это будет начальное минимальное значение.
    min_dif_heights = abs(user.height - athletes[0].height)
    # И атлета запоминаем:
    athlete_nearby_height = athletes[0]
    # print(min_dif_heights)
    # 
    for athlete in athletes:
        # У некоторых атлетов не указан рост, пропускаем их:
        if athlete.height is None:
            continue
        # Также считаем разницу и если она меньше текущей минимальной,
        # то переписываем её и атлета у которого она обнаружена:
        dif_heights = abs(user.height - athlete.height)
        if dif_heights < min_dif_heights:
            min_dif_heights = dif_heights
            athlete_nearby_height = athlete

    print("min_dif_heights:", min_dif_heights)
    print("athlete_nearby_height:", athlete_nearby_height.sport)    

def main():
    session = connect_db()

    user_id = input("Enter user id: ")

    user = session.query(User).filter(User.id == user_id).first()
    if user is None:
        print("No user with id:", user_id)
        return

    find_athlete_nearby_height(session, user)


if __name__ == "__main__":
    main()
