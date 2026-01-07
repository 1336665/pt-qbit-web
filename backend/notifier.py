#!/usr/bin/env python3
"""
通知模块 - 支持Telegram等通知方式
"""

import threading
import queue
import time
import logging
from typing import Optional, Dict, Any

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False


class Notifier:
    """通知管理器"""
    
    def __init__(self, db=None):
        self.db = db
        self._queue = queue.Queue()
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self.logger = logging.getLogger("notifier")
    
    def start(self):
        """启动通知线程"""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
    
    def stop(self):
        """停止通知线程"""
        self._running = False
        if self._thread:
            self._queue.put(None)  # 发送退出信号
            self._thread.join(timeout=5)
    
    def _run(self):
        """通知发送线程"""
        while self._running:
            try:
                item = self._queue.get(timeout=1)
                if item is None:
                    break
                
                self._send(item)
                
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"通知发送失败: {e}")
    
    def _send(self, notification: Dict[str, Any]):
        """发送通知"""
        if not REQUESTS_AVAILABLE:
            return
        
        # 获取Telegram配置
        if self.db:
            bot_token = self.db.get_config('telegram_bot_token', '')
            chat_id = self.db.get_config('telegram_chat_id', '')
        else:
            return
        
        if not bot_token or not chat_id:
            return
        
        title = notification.get('title', '')
        message = notification.get('message', '')
        text = f"*{title}*\n{message}" if title else message
        
        try:
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            payload = {
                'chat_id': chat_id,
                'text': text,
                'parse_mode': 'Markdown'
            }
            
            # 使用代理（如果配置了）
            proxies = {}
            if self.db:
                proxy = self.db.get_config('global_proxy', '')
                if proxy:
                    proxies = {'http': proxy, 'https': proxy}
            
            response = requests.post(url, json=payload, proxies=proxies, timeout=10)
            
            if response.status_code != 200:
                self.logger.warning(f"Telegram通知失败: {response.text}")
                
        except Exception as e:
            self.logger.error(f"发送Telegram通知失败: {e}")
    
    def notify(self, title: str = '', message: str = '', **kwargs):
        """添加通知到队列"""
        self._queue.put({
            'title': title,
            'message': message,
            **kwargs
        })
    
    def notify_startup(self):
        """发送启动通知"""
        self.notify(
            title="🚀 qBit Smart Web 已启动",
            message="服务已成功启动运行"
        )
    
    def notify_limit_applied(self, torrent_name: str, limit: int, 
                              reason: str = ''):
        """发送限速通知"""
        limit_str = f"{limit / 1024 / 1024:.2f} MiB/s" if limit > 0 else "无限制"
        msg = f"种子: {torrent_name[:30]}\n限速: {limit_str}"
        if reason:
            msg += f"\n原因: {reason}"
        
        self.notify(
            title="⚡ 限速已应用",
            message=msg
        )
    
    def notify_torrent_added(self, torrent_name: str, site_name: str = ''):
        """发送种子添加通知"""
        msg = f"种子: {torrent_name[:40]}"
        if site_name:
            msg += f"\n站点: {site_name}"
        
        self.notify(
            title="📥 种子已添加",
            message=msg
        )
    
    def notify_torrent_removed(self, torrent_name: str, reason: str = ''):
        """发送种子删除通知"""
        msg = f"种子: {torrent_name[:40]}"
        if reason:
            msg += f"\n原因: {reason}"
        
        self.notify(
            title="🗑️ 种子已删除",
            message=msg
        )
    
    def notify_error(self, error: str, context: str = ''):
        """发送错误通知"""
        msg = error
        if context:
            msg = f"[{context}] {error}"
        
        self.notify(
            title="❌ 错误",
            message=msg
        )


# 工厂函数
def create_notifier(db=None) -> Notifier:
    return Notifier(db)
