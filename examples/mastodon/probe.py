import requests

paths = [
    "/home",
    "/@lev",
    "/@lev/following",

    # Requires auth token
    "/settings/profile",
    "/settings/preferences/appearance",
    "/settings/preferences/notifications",
    "/settings/preferences/other",
    "/relationships",
    "/relationships?relationship=followed_by",
    "/relationships?relationship=followed_by&status=primary",
    "/relationships?activity=dormant&relationship=followed_by&status=primary",
    "/relationships?activity=dormant&order=active&relationship=followed_by&status=primary"\

    "/explore",
    "/public/local",
    "/conversations",
]

def probe():
    for path in paths:
        requests.get(f"http://localhost:3000{path}")

if __name__ == "__main__":
    counter = 0
    while True:
        if counter % 1000 == 0:
            print(f"Sent {counter} probes")
        probe()
        counter += 1
