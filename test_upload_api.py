import requests

url = "http://file.timhieuluat.com/upload.php"
secret = "Hotromt2012!"
file_content = b"This is a test file."

files = {
    'file': ('test.doc', file_content, 'application/octet-stream')
}
data = {
    'secret': secret
}

try:
    response = requests.post(url, data=data, files=files)
    print("Status Code:", response.status_code)
    print("Response JSON:", response.text)
except Exception as e:
    print("Error:", e)
