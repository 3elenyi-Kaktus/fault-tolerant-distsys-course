import requests


def fill(server_id: int):
    url = f'http://localhost:3333{server_id}/storage'
    payload = {'key': 'a', 'value': 1}
    res = requests.post(url, json=payload)

    payload = {'key': 'b', 'value': 2}
    res = requests.post(url, json=payload)

    payload = {'key': 'c', 'value': 3}
    res = requests.post(url, json=payload)

    payload = {'key': 'd', 'value': 4}
    res = requests.post(url, json=payload)

    res = requests.get(url + "?key=d")
    res = requests.get(url + "?key=c")
    res = requests.get(url + "?key=b")
    res = requests.get(url + "?key=a")


    payload = {'key': 'a', 'value': 42}
    res = requests.put(url, json=payload)

    payload = {'key': 'd', 'value': 123}
    res = requests.put(url, json=payload)


if __name__ == '__main__':
    fill(2)