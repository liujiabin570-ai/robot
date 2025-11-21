# WeChat Robot Agent（Flask + LangGraph）

面向微信群的业务助手与查询系统，后端基于 Flask，集成 LangGraph 的 ReAct Agent 与 Text-to-SQL 能力；支持 WorkTool 回调接入、仪表盘可视化与 MySQL 数据存储。

## 功能概览
- 消息接入：WorkTool 回调到后端 `/get_news`，兼容 GET 握手与 POST 消息
- 业务处理：支持模板消息（新家长/补全微信号/合伙人接手/转销售/销售接手/反馈/成交/流失）
- 查询回答：自然语言查询→SQL→执行→总结
- 数据入库：原始消息 `RawMessages`、家长与联系方式、流程日志、反馈记录
- 仪表盘：`/dashboard` 展示总览、明细与筛选

## 快速开始
1) 环境准备
- Python ≥ 3.12，MySQL ≥ 5.7/8.0
- 安装依赖：`pip install -r requirements.txt`

2) 配置环境变量（在项目根目录创建 `.env`）
```
MYSQL_HOST=127.0.0.1
MYSQL_USER=你的用户名
MYSQL_PASSWORD=你的密码
MYSQL_DB=lead_management
MYSQL_PORT=3306

FLASK_HOST=0.0.0.0
FLASK_PORT=5002
FLASK_DEBUG=False
SECRET_KEY=请自定义

OPENAI_API_KEY=按需填写（可留空）
OPENAI_BASE_URL=https://api.moonshot.cn/v1
OPENAI_MODEL=kimi-k2-turbo-preview
SQL_AGENT_MODE=direct

WORKTOOL_API_HOST=api.worktool.ymdyes.cn
WORKTOOL_ROBOT_ID=你的机器人ID
```

3) 初始化数据库
- 方式一：Python 交互
```
python -c "from models.database import create_tables; create_tables()"
```
- 方式二：脚本（如有）：`python scripts/init_database.py`

4) 启动（开发）
- `python run.py`
- 健康检查：`GET /health`
- 仪表盘：`GET /dashboard`

## 生产部署（Linux）
1) 后端（WSGI）
- `pip install gunicorn gevent`
- `gunicorn -w 2 -k gevent -b 127.0.0.1:5002 app:app`

2) Nginx 反向代理（HTTP 示例）
```
server {
    listen 80;
    server_name 51talk.website www.51talk.website;

    location = / { return 302 /dashboard; }
    location /dashboard { proxy_pass http://127.0.0.1:5002/dashboard; }
    location /get_news  { proxy_pass http://127.0.0.1:5002/get_news; }
    location /health    { proxy_pass http://127.0.0.1:5002/health; }
    location /api/      { proxy_pass http://127.0.0.1:5002; }
}
```
- 如需 HTTPS：配置证书后将 `server` 改为 `listen 443 ssl;` 并把 80 站点重定向到 443

## WorkTool 回调配置
- 推荐脚本：`python get_news.py`（将 `callbackUrl` 指向你的域名）
- 或使用 curl：
```
curl -s -X POST 'https://api.worktool.ymdyes.cn/robot/robotInfo/update?robotId=你的机器人ID&key=' \
  -H 'Content-Type: application/json' \
  -d '{
    "openCallback":1,
    "replyAll":1,
    "callbackUrl":"http://www.51talk.website/get_news",
    "groupName":"测试"
  }'
```
- 验证：`curl http://www.51talk.website/get_news` 返回 `{"status":"ok"}`

## 主要接口
- `GET /health` 服务健康状态（`d:\AI Work\robot\app.py:35-42`）
- `POST /get_news` WorkTool 回调入口（`d:\AI Work\robot\app.py:44-120`）
- `GET /dashboard` 仪表盘页面（`d:\AI Work\robot\app.py:586-591`）
- `GET /api/parents` 家长列表（`d:\AI Work\robot\app.py:549-584`）

## 注意事项
- `.env` 含敏感信息，已在 `.gitignore` 中忽略，请勿提交到仓库
- 若出现 HTTPS 推送到 WorkTool 的证书校验失败，已在 `send_message.py` 使用 `requests+certifi` 绑定 CA
- 管理页编辑时，枚举字段的“未设置”会写入 `NULL`，避免数据库枚举类型错误（`d:\AI Work\robot\app.py:936-978`）