import requests

url = "http://localhost:3000"
token = "P48t7hDUJUDHWnaIdlEiGZvd0lpcuzWUfhmGu2e7jqk"

def post():
    post = requests.post(f"{url}/api/v1/statuses", headers={
        "Authorization": f"Bearer {token}"
    }, json={
        "status": "Hey!",
    })
    print(post.text)

def read():
    convos = requests.get(f"{url}/api/v1/statuses", headers={
        "Authorization": f"Bearer {token}"
    })
    assert convos.status_code == 200

    user = requests.get(f"{url}/@lev.json")
    assert user.status_code == 200

if __name__ == "__main__":
    post()
    read()
