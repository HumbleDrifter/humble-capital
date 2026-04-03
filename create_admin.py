from getpass import getpass
from storage import init_user_table, create_user


def main():
    init_user_table()

    username = input("Username: ").strip().lower()
    email = input("Email (optional): ").strip() or None
    password = getpass("Password: ")
    confirm = getpass("Confirm Password: ")

    if password != confirm:
        print("Passwords do not match.")
        return

    try:
        user_id = create_user(
            username=username,
            password=password,
            email=email,
            is_admin=1
        )
        print(f"Admin user created with id={user_id}")
    except Exception as exc:
        print(f"Failed to create user: {exc}")


if __name__ == "__main__":
    main()
