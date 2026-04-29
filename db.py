from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

DB_URL = "mysql+pymysql://{}:{}@{}:{}/{}".format(
    os.getenv("user"),
    os.getenv("password"),
    os.getenv("host"),
    os.getenv("port"),
    os.getenv("database")
)

engine = create_engine(DB_URL)