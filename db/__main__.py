from .db import init_db

if __name__ == "__main__":
    path = init_db()
    print(f"Initialized {path}")
