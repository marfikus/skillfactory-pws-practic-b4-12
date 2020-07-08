
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

def request_user_data():
    def valid_email(email):
        # делим по собаке, должно получиться 2 куска, если нормальный емейл:
        email_splitted = email.split("@")
        if len(email_splitted) != 2:
            return False
        
        # отсекаем ситуацию, когда собака в начале или конце:
        name, domain = email_splitted
        if (len(name) == 0) or (len(domain) == 0):
            return False

        # наличие точки в домене:
        if not "." in domain:
            return False

        # и также отсекаем ситуацию, когда точка в начале или в конце:
        if (domain[0] == ".") or (domain[-1] == "."):
            return False

        return True

    def valid_birthdate(birthdate):
        # делим полученную строку, должно получиться 3 куска:
        # birthdate_splitted = birthdate.split("-")
        # if len(birthdate_splitted) != 3:
            # return False

        # проверяем куски: числа ли это?
        # for chunk in birthdate_splitted:
            # try:
                # number = int(chunk)
            # except ValueError:
                # return False
        
        # Пробуем преобразовать строку в формат даты,
        # если она некорректная, то отвергаем её.
        # (Таким образом, предыдущие проверки уже не нужны)
        try:
            dt.datetime.strptime(birthdate, "%Y-%m-%d")
        except ValueError:
            return False

        return True 

    first_name = input("Enter first name: ")
    last_name = input("Enter last name: ")
    gender = input("Enter gender: ")

    email = input("Enter email: ")
    while not valid_email(email):
        email = input("Incorrect email! Enter email: ")

    birthdate = input("Enter birthdate(format: year-mm-dd): ")
    while not valid_birthdate(birthdate):
        birthdate = input("Incorrect birthdate! Enter birthdate(format: year-mm-dd): ")

    # проверка ввода роста:
    while True:
        try:
            height = float(input("Enter height: "))
            break
        except ValueError as er:
            print("Incorrect value!", er)

    user = User(
        first_name=first_name,
        last_name=last_name,
        gender=gender,
        email=email,
        birthdate=birthdate,
        height=height
    )

    return user

def main():
    session = connect_db()

    user = request_user_data()
    session.add(user)

    session.commit()
    print("User added")

if __name__ == "__main__":
    main()