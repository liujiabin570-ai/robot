from flask import Flask, request, jsonify

app = Flask(__name__)


@app.route('/get_news', methods=['GET', 'POST'])  # ← 关键：同时支持 GET 和 POST
def third_qa():
    # 如果是 GET 请求（测试连通性）
    if request.method == 'GET':
        return jsonify({"status": "ok", "message": "接口正常"})

    # 如果是 POST 请求（实际回调）
    data = request.json
    print("接收到的消息：", data)

    # 提取消息内容（根据实际字段调整）
    user_message = data.get('content', '') or data.get('message', '') or data.get('text', '')

    # 简单回复
    reply_content = f"收到消息：{user_message}"


    # 返回格式（可能需要根据文档调整）
    return jsonify({
        "content": reply_content  # 或 "message" / "reply"，看文档要求
    })


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5002)
