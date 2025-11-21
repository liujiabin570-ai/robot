import http.client
import json
from typing import Optional
from config import config
from utils.logger import app_logger
import requests
try:
    import certifi
    _VERIFY = certifi.where()
except Exception:
    _VERIFY = True


def send_group_text(content: str, group_name: Optional[str] = None) -> dict:
    """发送群文本消息到 WorkTool 指定群
    content: 要发送的文本内容
    group_name: 目标群名称（通过titleList定向）
    返回：接口响应字典
    """
    try:
        host = getattr(config, 'WORKTOOL_API_HOST', 'api.worktool.ymdyes.cn')
        robot_id = (getattr(config, 'WORKTOOL_ROBOT_ID', '') or '').strip()
        if not robot_id:
            return {"success": False, "error": "缺少WORKTOOL_ROBOT_ID配置"}
        if not group_name:
            return {"success": False, "error": "缺少目标群名称(group_name)"}

        url = f"https://{host}/wework/sendRawMessage?robotId={robot_id}"
        payload = {
            "socketType": 2,
            "list": [
                {
                    "type": 203,
                    # WorkTool 收件人（群）列表
                    "titleList": [group_name],
                    "receivedContent": content
                }
            ]
        }
        headers = {
            'Content-Type': 'application/json'
        }
        # 日志：准备发送
        app_logger.info(f"WorkTool群推送: host={host}, robot_id={robot_id}, group={group_name}, content_len={len(content)}")
        app_logger.debug(f"WorkTool payload: {payload}")

        res = requests.post(url, json=payload, headers=headers, timeout=15, verify=_VERIFY)
        status = getattr(res, 'status_code', None)
        reason = getattr(res, 'reason', '')
        data = res.text

        # 解析响应
        try:
            parsed = json.loads(data)
        except Exception:
            parsed = {"raw": data}

        app_logger.info(f"WorkTool推送完成: http_status={status} reason={reason} resp={parsed}")

        return {
            "success": True if status and 200 <= status < 300 else False,
            "http_status": status,
            "reason": reason,
            "response": parsed
        }
    except Exception as e:
        app_logger.error(f"WorkTool推送异常: {e}")
        return {"success": False, "error": str(e)}


if __name__ == '__main__':
    # 简单自测（需设置 WORKTOOL_ROBOT_ID 和目标群名）
    print(send_group_text("测试消息", group_name="测试"))