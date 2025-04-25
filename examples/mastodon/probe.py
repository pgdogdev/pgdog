import requests
from mastodon import Mastodon

url = "http://localhost:3000"
token = "Ig_tfErP7PsiBtpN8QwQkUJDYNEcfLSWCkXGkGXA5oM"

# sNkX7Q-1CCSjGUpTKdvFr4_sZM5sSDPO15Qhb-VOQpI

# def login():
#     auth = requests.get("http://localhost:3000/oauth/authorize?client_id=zJEuvDP_LOim8QWlpATxJBISkLF2-Rf0aPuY1JClQsw&redirect_uri=urn:ietf:wg:oauth:2.0:oob&response_type=code")
#     print(auth.text)
#
def login():
    token = requests.post(f"{url}/oauth/token", json={
        "grant_type": "authorization_code",
        "code": "Ig_tfErP7PsiBtpN8QwQkUJDYNEcfLSWCkXGkGXA5oM",
        "client_id": "zJEuvDP_LOim8QWlpATxJBISkLF2-Rf0aPuY1JClQsw",
        "client_secret": "9gV_cXgbUk9q42_4XvlgLB83ibqg3j0S9L3fejtNAic",
        "redirect_uri": "urn:ietf:wg:oauth:2.0:oob"
    })
    print(token.text)

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
    # print(convos.json())

# paths = [
#     "/home",
#     "/@lev",
#     "/@lev/following",

#     # Requires auth token
#     "/settings/profile",
#     "/settings/preferences/appearance",
#     "/settings/preferences/notifications",
#     "/settings/preferences/other",
#     "/relationships",
#     "/relationships?relationship=followed_by",
#     "/relationships?relationship=followed_by&status=primary",
#     "/relationships?activity=dormant&relationship=followed_by&status=primary",
#     "/relationships?activity=dormant&order=active&relationship=followed_by&status=primary"\

#     "/explore",
#     "/public/local",
#     "/conversations",
# ]

# def probe():
#     for path in paths:
#         requests.get(f"http://localhost:3000{path}")

# if __name__ == "__main__":
#     counter = 0
#     while True:
#         if counter % 1000 == 0:
#             print(f"Sent {counter} probes")
#         probe()
#         counter += 1

if __name__ == "__main__":
    post()
    # read()
