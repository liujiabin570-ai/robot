import http.client
import json

conn = http.client.HTTPSConnection("api.worktool.ymdyes.cn")
payload = json.dumps({
   "openCallback": 1,
   "replyAll": 1,
   "callbackUrl": "http://www.51talk.website/get_news",
   "groupName": "测试",
})
headers = {
   'Content-Type': 'application/json'
}
conn.request("POST", "/robot/robotInfo/update?robotId=wcwalws3pg6jg5ces081fudvs6j34p62&key=", payload, headers)
res = conn.getresponse()
data = res.read()
print(data.decode("utf-8"))