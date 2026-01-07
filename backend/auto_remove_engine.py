#!/usr/bin/env python3
"""
自动删种引擎 v1.8

修复:
- 改进规则匹配逻辑
- 添加更多日志
"""

import time
import json
import threading
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass


@dataclass
class RemoveRecord:
    timestamp: float
    instance_id: int
    instance_name: str
    torrent_hash: str
    torrent_name: str
    rule_name: str
    reason: str
    size: int
    uploaded: int
    ratio: float


class AutoRemoveEngine:
    def __init__(self, db, qb_manager, notifier=None):
        self.db = db
        self.qb_manager = qb_manager
        self.notifier = notifier
        self.logger = logging.getLogger("auto_remove")
        
        self._running = False
        self._thread = None
        self._stop_event = threading.Event()
        
        self._check_interval = 60
        self._sleep_between = 5
        self._enabled = False
        self._reannounce_before_delete = True
        self._delete_files = True  # 新增：是否删除文件
        
        self._remove_records = []
        self._max_records = 500
        
        self._total_removed = 0
        self._total_freed = 0
    
    def start(self):
        if self._running:
            return
        
        self._load_config()
        
        if not self._enabled:
            self.logger.info("自动删种已禁用")
            return
        
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._worker, daemon=True, name="AutoRemove")
        self._thread.start()
        self.logger.info(f"自动删种引擎已启动 (间隔: {self._check_interval}秒)")
        self._log_db('INFO', '自动删种引擎已启动')
    
    def stop(self):
        self._running = False
        self._stop_event.set()
        self.logger.info("自动删种引擎已停止")
        self._log_db('INFO', '自动删种引擎已停止')
    
    def _load_config(self):
        self._enabled = self.db.get_config('auto_remove_enabled') == 'true'
        try:
            self._check_interval = int(self.db.get_config('auto_remove_interval') or 60)
        except:
            self._check_interval = 60
        try:
            self._sleep_between = int(self.db.get_config('auto_remove_sleep') or 5)
        except:
            self._sleep_between = 5
        self._reannounce_before_delete = self.db.get_config('auto_remove_reannounce') != 'false'
        # 默认删除文件，除非明确设置为false
        self._delete_files = self.db.get_config('auto_remove_delete_files') != 'false'
    
    def _log_db(self, level: str, message: str):
        try:
            self.db.add_log(level, f"[删种] {message}")
        except:
            pass
    
    def get_status(self) -> Dict:
        return {
            'running': self._running,
            'enabled': self._enabled,
            'check_interval': self._check_interval,
            'sleep_between': self._sleep_between,
            'reannounce_before_delete': self._reannounce_before_delete,
            'delete_files': self._delete_files,
            'total_removed': self._total_removed,
            'total_freed': self._total_freed,
            'recent_records': len(self._remove_records)
        }
    
    def get_records(self, limit: int = 100) -> List[Dict]:
        records = self._remove_records[-limit:]
        return [{
            'time': datetime.fromtimestamp(r.timestamp).strftime('%Y-%m-%d %H:%M:%S'),
            'instance': r.instance_name,
            'name': r.torrent_name[:50] + '...' if len(r.torrent_name) > 50 else r.torrent_name,
            'rule': r.rule_name,
            'reason': r.reason,
            'size': r.size,
            'uploaded': r.uploaded,
            'ratio': r.ratio
        } for r in reversed(records)]
    
    def set_config(self, interval: int = None, sleep_between: int = None, 
                   reannounce: bool = None, enabled: bool = None, delete_files: bool = None):
        if interval is not None:
            self._check_interval = max(30, min(3600, interval))
            self.db.set_config('auto_remove_interval', str(self._check_interval))
        if sleep_between is not None:
            self._sleep_between = max(1, min(60, sleep_between))
            self.db.set_config('auto_remove_sleep', str(self._sleep_between))
        if reannounce is not None:
            self._reannounce_before_delete = reannounce
            self.db.set_config('auto_remove_reannounce', 'true' if reannounce else 'false')
        if enabled is not None:
            self._enabled = enabled
            self.db.set_config('auto_remove_enabled', 'true' if enabled else 'false')
        if delete_files is not None:
            self._delete_files = delete_files
            self.db.set_config('auto_remove_delete_files', 'true' if delete_files else 'false')
    
    def _worker(self):
        while self._running and not self._stop_event.is_set():
            try:
                self._load_config()
                
                if self._enabled:
                    self._check_and_remove()
            except Exception as e:
                self.logger.error(f"删种检查异常: {e}")
                self._log_db('ERROR', f'检查异常: {e}')
            
            self._stop_event.wait(self._check_interval)
    
    def _check_and_remove(self):
        rules = self.db.get_enabled_remove_rules()
        if not rules:
            return
        
        instances = self.db.get_qb_instances()
        
        for inst in instances:
            if not inst.get('enabled'):
                continue
            
            inst_id = inst['id']
            if not self.qb_manager.is_connected(inst_id):
                continue
            
            free_space = self.qb_manager.get_free_space(inst_id)
            torrents = self.qb_manager.get_torrents(inst_id)
            
            for torrent in torrents:
                matched_rule = self._match_rules(torrent, rules, free_space)
                if matched_rule:
                    self._remove_torrent(inst, torrent, matched_rule, free_space)
                    
                    if self._sleep_between > 0:
                        time.sleep(self._sleep_between)
                    
                    if not self._running:
                        return
    
    def _match_rules(self, torrent: Dict, rules: List[Dict], free_space: int) -> Optional[Dict]:
        for rule in rules:
            try:
                condition = json.loads(rule.get('condition', '{}'))
            except:
                continue
            
            if self._check_condition(torrent, condition, free_space):
                return rule
        
        return None
    
    def _check_condition(self, torrent: Dict, condition: Dict, free_space: int) -> bool:
        # 剩余空间条件
        if 'free_space_lt' in condition:
            if free_space >= condition['free_space_lt']:
                return False
        
        # 上传速度条件
        if 'upload_speed_lt' in condition:
            up_speed = torrent.get('upspeed', 0)
            if up_speed >= condition['upload_speed_lt']:
                return False
        
        # 已完成条件
        if condition.get('completed'):
            progress = torrent.get('progress', 0)
            if progress < 1.0:
                return False
        
        # 做种时间条件
        if 'seeding_time_gt' in condition:
            seeding_time = torrent.get('seeding_time', 0)
            if seeding_time <= condition['seeding_time_gt']:
                return False
        
        # 分享率条件
        if 'ratio_gt' in condition:
            ratio = torrent.get('ratio', 0)
            if ratio <= condition['ratio_gt']:
                return False
        
        # 种子大小条件
        if 'size_gt' in condition:
            size = torrent.get('size', 0)
            if size <= condition['size_gt']:
                return False
        
        # 无连接时间条件
        if 'no_peers_time_gt' in condition:
            last_activity = torrent.get('last_activity', 0)
            if last_activity > 0:
                no_peer_time = time.time() - last_activity
                if no_peer_time <= condition['no_peers_time_gt']:
                    return False
        
        return True
    
    def _remove_torrent(self, instance: Dict, torrent: Dict, rule: Dict, free_space: int):
        inst_id = instance['id']
        inst_name = instance['name']
        torrent_hash = torrent.get('hash', '')
        torrent_name = torrent.get('name', 'Unknown')
        
        # 删前汇报
        if self._reannounce_before_delete:
            try:
                self.qb_manager.reannounce(inst_id, torrent_hash)
                self.logger.info(f"[{inst_name}] 删前汇报: {torrent_name[:30]}")
                time.sleep(2)
            except Exception as e:
                self.logger.warning(f"汇报失败: {e}")
        
        # 执行删除（使用配置决定是否删除文件）
        self.logger.info(f"[{inst_name}] 准备删除: {torrent_name[:30]} (删除文件: {self._delete_files})")
        success, msg = self.qb_manager.delete_torrent(inst_id, torrent_hash, delete_files=self._delete_files)
        
        if success:
            size = torrent.get('size', 0)
            uploaded = torrent.get('uploaded', 0)
            ratio = torrent.get('ratio', 0)
            
            record = RemoveRecord(
                timestamp=time.time(),
                instance_id=inst_id,
                instance_name=inst_name,
                torrent_hash=torrent_hash,
                torrent_name=torrent_name,
                rule_name=rule['name'],
                reason=rule.get('description', ''),
                size=size,
                uploaded=uploaded,
                ratio=ratio
            )
            self._remove_records.append(record)
            
            if len(self._remove_records) > self._max_records:
                self._remove_records = self._remove_records[-self._max_records:]
            
            self._total_removed += 1
            self._total_freed += size
            
            self.logger.info(f"[{inst_name}] 删除: {torrent_name[:30]} | 规则: {rule['name']}")
            self._log_db('INFO', f"删除 [{torrent_name[:30]}] 规则:{rule['name']} 大小:{self._fmt_size(size)}")
            
            if self.notifier:
                try:
                    self.notifier.notify(
                        title="🗑️ 自动删种",
                        message=f"📦 {torrent_name[:40]}\n📏 {self._fmt_size(size)}\n📊 分享率: {ratio:.2f}\n📋 规则: {rule['name']}"
                    )
                except:
                    pass
        else:
            self.logger.error(f"[{inst_name}] 删除失败: {torrent_name[:30]} - {msg}")
            self._log_db('ERROR', f"删除失败 [{torrent_name[:30]}]: {msg}")
    
    def _fmt_size(self, b: int) -> str:
        for u in ['B', 'KiB', 'MiB', 'GiB', 'TiB']:
            if abs(b) < 1024:
                return f"{b:.2f} {u}"
            b /= 1024
        return f"{b:.2f} PiB"
    
    def manual_check(self) -> Dict:
        if not self._running:
            return {'success': False, 'message': '引擎未运行'}
        
        try:
            self._check_and_remove()
            return {'success': True, 'message': '检查完成'}
        except Exception as e:
            return {'success': False, 'message': str(e)}


def create_auto_remove_engine(db, qb_manager, notifier=None) -> AutoRemoveEngine:
    return AutoRemoveEngine(db, qb_manager, notifier)
