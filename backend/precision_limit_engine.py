#!/usr/bin/env python3
"""
精准限速引擎 v1.8

修复:
- 修复 target_speed_kib 字段读取
- 修复 set_upload_limit 方法调用
- 改进日志和状态显示
"""

import time
import threading
import logging
from urllib.parse import urlparse
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Any, Tuple
from collections import deque

try:
    from pt_site_helper import PTSiteHelperManager, create_helper_manager
    PT_HELPER_AVAILABLE = True
except ImportError:
    PT_HELPER_AVAILABLE = False


# ════════════════════════════════════════════════════════════════════════════════
# 常量
# ════════════════════════════════════════════════════════════════════════════════
class LimitConfig:
    FINISH_TIME = 30
    STEADY_TIME = 120
    WARMUP_TIME = 300
    
    PID_PARAMS = {
        'warmup': {'kp': 0.3, 'ki': 0.05, 'kd': 0.02, 'headroom': 1.03},
        'catch':  {'kp': 0.5, 'ki': 0.10, 'kd': 0.05, 'headroom': 1.02},
        'steady': {'kp': 0.6, 'ki': 0.15, 'kd': 0.08, 'headroom': 1.005},
        'finish': {'kp': 0.8, 'ki': 0.20, 'kd': 0.12, 'headroom': 1.001},
    }
    
    MIN_LIMIT = 4096
    MAX_LIMIT = 500 * 1024 * 1024
    
    ANNOUNCE_INTERVAL_NEW = 1800
    ANNOUNCE_INTERVAL_WEEK = 2700
    ANNOUNCE_INTERVAL_OLD = 3600
    
    LOG_INTERVAL = 20


# ════════════════════════════════════════════════════════════════════════════════
# 工具函数
# ════════════════════════════════════════════════════════════════════════════════
def safe_div(a: float, b: float, default: float = 0) -> float:
    try:
        if b == 0 or abs(b) < 1e-10:
            return default
        return a / b
    except:
        return default

def clamp(value: float, min_val: float, max_val: float) -> float:
    return max(min_val, min(max_val, value))

def fmt_speed(b: float) -> str:
    if b == 0:
        return "0 B/s"
    for u in ['B/s', 'KiB/s', 'MiB/s', 'GiB/s']:
        if abs(b) < 1024:
            return f"{b:.1f} {u}"
        b /= 1024
    return f"{b:.1f} TiB/s"

def fmt_size(b: float) -> str:
    if b == 0:
        return "0 B"
    for u in ['B', 'KiB', 'MiB', 'GiB', 'TiB']:
        if abs(b) < 1024:
            return f"{b:.2f} {u}"
        b /= 1024
    return f"{b:.2f} PiB"

def get_phase(time_left: float, cycle_synced: bool) -> str:
    if not cycle_synced:
        return 'warmup'
    if time_left <= LimitConfig.FINISH_TIME:
        return 'finish'
    if time_left <= LimitConfig.STEADY_TIME:
        return 'steady'
    return 'catch'


# ════════════════════════════════════════════════════════════════════════════════
# PID控制器
# ════════════════════════════════════════════════════════════════════════════════
class PIDController:
    def __init__(self):
        self.integral = 0
        self.last_error = 0
        self.last_time = 0
        self.phase = 'warmup'
    
    def set_phase(self, phase: str):
        if phase != self.phase:
            self.integral *= 0.5
            self.phase = phase
    
    def update(self, target: float, actual: float, now: float) -> float:
        params = LimitConfig.PID_PARAMS.get(self.phase, LimitConfig.PID_PARAMS['catch'])
        kp, ki, kd = params['kp'], params['ki'], params['kd']
        
        error = safe_div(target - actual, max(target, 1), 0)
        
        dt = now - self.last_time if self.last_time > 0 else 1
        self.last_time = now
        
        self.integral = clamp(self.integral + error * dt, -0.5, 0.5)
        
        derivative = (error - self.last_error) / dt if dt > 0 else 0
        self.last_error = error
        
        output = 1.0 + kp * error + ki * self.integral + kd * derivative
        return clamp(output, 0.3, 3.0)
    
    def reset(self):
        self.integral = 0
        self.last_error = 0
        self.last_time = 0


# ════════════════════════════════════════════════════════════════════════════════
# Kalman滤波器
# ════════════════════════════════════════════════════════════════════════════════
class KalmanFilter:
    def __init__(self):
        self.speed = 0
        self.acceleration = 0
        self.p_speed = 1
        self.p_accel = 1
        self.last_time = 0
        
        self.q_speed = 0.1
        self.q_accel = 0.05
        self.r = 0.5
    
    def update(self, measured_speed: float, now: float):
        if self.last_time <= 0:
            self.speed = measured_speed
            self.last_time = now
            return
        
        dt = now - self.last_time
        if dt <= 0:
            return
        self.last_time = now
        
        predicted_speed = self.speed + self.acceleration * dt
        self.p_speed += self.q_speed + self.p_accel * dt * dt
        self.p_accel += self.q_accel
        
        innovation = measured_speed - predicted_speed
        k = self.p_speed / (self.p_speed + self.r)
        
        self.speed = predicted_speed + k * innovation
        self.acceleration += 0.1 * innovation / dt
        self.p_speed *= (1 - k)
    
    def predict_upload(self, time_left: float) -> float:
        return self.speed * time_left + 0.5 * self.acceleration * time_left * time_left
    
    def reset(self):
        self.speed = 0
        self.acceleration = 0
        self.p_speed = 1
        self.p_accel = 1
        self.last_time = 0


# ════════════════════════════════════════════════════════════════════════════════
# 种子状态
# ════════════════════════════════════════════════════════════════════════════════
@dataclass
class TorrentLimitState:
    """单个种子的限速状态"""
    hash: str
    name: str = ""
    tracker: str = ""
    instance_id: int = 0
    
    cycle_start: float = 0
    cycle_uploaded_start: int = 0
    cycle_index: int = 0
    cycle_synced: bool = False
    
    reannounce_time: float = 0
    cached_time_left: float = 1800
    reannounce_source: str = "unknown"
    
    target_speed: int = 50 * 1024 * 1024
    last_limit: int = -1
    last_limit_reason: str = ""
    
    site_id: Optional[int] = None
    tid: Optional[int] = None
    
    pid: PIDController = field(default_factory=PIDController)
    kalman: KalmanFilter = field(default_factory=KalmanFilter)
    
    last_log_time: float = 0
    
    def get_phase(self, now: float) -> str:
        if not self.cycle_synced:
            return 'warmup'
        time_left = max(0, self.reannounce_time - now) if self.reannounce_time > 0 else self.cached_time_left
        return get_phase(time_left, self.cycle_synced)
    
    def get_cycle_uploaded(self, current_uploaded: int) -> int:
        return max(0, current_uploaded - self.cycle_uploaded_start)
    
    def new_cycle(self, now: float, current_uploaded: int, time_left: float):
        self.cycle_start = now
        self.cycle_uploaded_start = current_uploaded
        self.cycle_index += 1
        self.pid.reset()
        self.reannounce_time = now + time_left
        self.cached_time_left = time_left


# ════════════════════════════════════════════════════════════════════════════════
# 精准限速引擎
# ════════════════════════════════════════════════════════════════════════════════
class PrecisionLimitEngine:
    """精准限速引擎"""
    
    VERSION = "1.8.0"
    
    def __init__(self, db, qb_manager, site_helper_manager=None, notifier=None, logger=None):
        self.db = db
        self.qb_manager = qb_manager
        self.site_helper_manager = site_helper_manager
        self.notifier = notifier
        self.logger = logger or logging.getLogger("limit_engine")
        
        self._states: Dict[str, TorrentLimitState] = {}
        self._running = False
        self._thread = None
        self._lock = threading.Lock()
        
        self._stats = {
            'site_success': 0,
            'qb_api_success': 0,
            'fallback_count': 0,
            'torrents_controlled': 0,
        }
        
        # 状态持久化相关
        self._last_save_time = 0
        self._save_interval = 180  # 每3分钟保存一次
        
        # 尝试从数据库恢复状态
        self._restore_states_from_db()
    
    def _restore_states_from_db(self):
        """从数据库恢复种子限速状态"""
        try:
            saved_states = self.db.get_all_torrent_limit_states()
            restored = 0
            for data in saved_states:
                # 检查数据是否过期（超过24小时）
                if time.time() - data.get('updated_at', 0) > 86400:
                    continue
                
                state = TorrentLimitState(
                    hash=data['hash'],
                    name=data.get('name', ''),
                    tracker=data.get('tracker', ''),
                    instance_id=data.get('instance_id', 0),
                    site_id=data.get('site_id'),
                    tid=data.get('tid'),
                    cycle_index=data.get('cycle_index', 0),
                    cycle_start=data.get('cycle_start', 0),
                    cycle_uploaded_start=data.get('cycle_uploaded_start', 0),
                    cycle_synced=bool(data.get('cycle_synced')),
                    target_speed=data.get('target_speed', 0),
                    last_limit=data.get('last_limit', -1),
                    reannounce_time=data.get('reannounce_time', 0),
                    cached_time_left=data.get('cached_time_left', 1800),
                )
                self._states[data['hash']] = state
                restored += 1
            
            if restored > 0:
                self._log('info', f"📦 从数据库恢复了 {restored} 个种子的限速状态")
        except Exception as e:
            self._log('warning', f"恢复状态失败: {e}")
    
    def _save_states_to_db(self):
        """保存所有状态到数据库"""
        try:
            for hash, state in self._states.items():
                self.db.save_torrent_limit_state({
                    'hash': state.hash,
                    'name': state.name,
                    'tracker': state.tracker,
                    'instance_id': state.instance_id,
                    'site_id': state.site_id,
                    'tid': state.tid,
                    'cycle_index': state.cycle_index,
                    'cycle_start': state.cycle_start,
                    'cycle_uploaded_start': state.cycle_uploaded_start,
                    'cycle_synced': state.cycle_synced,
                    'target_speed': state.target_speed,
                    'last_limit': state.last_limit,
                    'reannounce_time': state.reannounce_time,
                    'cached_time_left': state.cached_time_left,
                })
            self._last_save_time = time.time()
        except Exception as e:
            self._log('warning', f"保存状态失败: {e}")
    
    def _log(self, level: str, message: str):
        level_name = level.lower()
        getattr(self.logger, level_name, self.logger.info)(f"[LimitEngine] {message}")
        if level_name in {"info", "warning", "error"}:
            try:
                self.db.add_log(level_name.upper(), f"[LimitEngine] {message}")
            except Exception:
                pass
    
    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self._log('info', f"精准限速引擎 v{self.VERSION} 已启动")
    
    def stop(self):
        self._running = False
        # 停止前保存状态
        self._save_states_to_db()
        if self._thread:
            self._thread.join(timeout=5)
        self._log('info', "精准限速引擎已停止")
    
    def is_running(self) -> bool:
        return self._running
    
    def _run_loop(self):
        while self._running:
            try:
                self._process_all()
                
                # 定期保存状态
                if time.time() - self._last_save_time > self._save_interval:
                    self._save_states_to_db()
                    
            except Exception as e:
                self._log('error', f"处理异常: {e}")
            time.sleep(5)

    def _should_limit_torrent(self, torrent: dict) -> bool:
        state = (torrent.get('state') or '').lower()
        if torrent.get('upspeed', 0) > 0:
            return True
        if not state:
            return False
        if 'upload' in state or 'seed' in state:
            return True
        return state.endswith('up')
    
    def _process_all(self):
        """处理所有活动种子"""
        now = time.time()
        
        # 更新站点辅助器配置
        if self.site_helper_manager and PT_HELPER_AVAILABLE:
            try:
                sites = self.db.get_pt_sites()
                proxy = self.db.get_config('global_proxy') or ''
                self.site_helper_manager.update_from_db(sites, proxy)
            except Exception as e:
                self._log('debug', f"更新站点配置失败: {e}")
        
        # 获取启用的限速规则
        enabled_rules = {}
        try:
            rules = self.db.get_speed_rules()
            for rule in rules:
                if rule.get('enabled'):
                    site_id = rule.get('site_id')
                    enabled_rules[site_id] = rule
        except Exception as e:
            self._log('debug', f"获取限速规则失败: {e}")
            return
        
        if not enabled_rules:
            self._log('warning', "未找到启用的限速规则")
            return
        
        # 处理每个qB实例
        instances = self.db.get_qb_instances()
        controlled_count = 0
        
        if not instances:
            self._log('warning', "未找到qB实例配置")
            return

        for instance in instances:
            if not instance['enabled']:
                continue
            
            inst_id = instance['id']
            client = self.qb_manager.get_client(inst_id)
            if not client:
                continue
            
            try:
                torrents = self.qb_manager.get_torrents(inst_id)
            except Exception as e:
                self._log('warning', f"获取种子列表失败: {e}")
                continue
            
            if not torrents:
                self._log('info', f"实例{inst_id}未返回任何种子")
                continue
            
            for torrent in torrents:
                if not self._should_limit_torrent(torrent):
                    continue
                rule = self._find_rule(torrent, enabled_rules)
                if rule:
                    self._process_torrent(inst_id, client, torrent, rule, now)
                    controlled_count += 1
                else:
                    self._log('info', f"未匹配到规则: {torrent.get('name', '')[:30]}")
        
        self._stats['torrents_controlled'] = controlled_count
    
    def _find_rule(self, torrent: dict, rules: Dict[int, dict]) -> Optional[dict]:
        """查找适用的限速规则"""
        tracker = torrent.get('tracker', '') or ''
        tracker_lower = tracker.lower()
        
        sites = self.db.get_pt_sites()
        for site in sites:
            keyword = site.get('tracker_keyword', '') or ''
            site_id = site.get('id')
            if site_id not in rules:
                continue
            if keyword and keyword.lower() in tracker_lower:
                return rules[site_id]
            site_url = site.get('url') or ''
            if site_url:
                site_host = (urlparse(site_url).hostname or '').lower()
                if site_host and site_host in tracker_lower:
                    return rules[site_id]
        
        return rules.get(None)
    
    def _process_torrent(self, instance_id: int, client, torrent: dict, rule: dict, now: float):
        """处理单个种子"""
        hash = torrent['hash']
        tracker = torrent.get('tracker', '')
        
        # 获取或创建状态
        if hash not in self._states:
            # 获取目标速度 (KiB/s -> B/s)
            target_kib = rule.get('target_speed_kib', 51200)
            safety = rule.get('safety_margin', 0.98)
            target_speed = int(target_kib * 1024 * safety)
            
            self._states[hash] = TorrentLimitState(
                hash=hash,
                name=torrent.get('name', '')[:30],
                tracker=tracker,
                instance_id=instance_id,
                cycle_start=now,
                cycle_uploaded_start=torrent.get('uploaded', 0),
                target_speed=target_speed,
            )
        
        state = self._states[hash]
        
        # 更新目标速度
        target_kib = rule.get('target_speed_kib', 51200)
        safety = rule.get('safety_margin', 0.98)
        state.target_speed = int(target_kib * 1024 * safety)
        state.tracker = tracker
        state.instance_id = instance_id
        
        # 获取当前数据
        current_uploaded = torrent.get('uploaded', 0)
        current_speed = torrent.get('upspeed', 0)
        
        # 更新Kalman滤波器
        state.kalman.update(current_speed, now)
        
        # 获取汇报时间
        time_left, source = self._get_reannounce_time(client, hash, tracker, state, now)
        state.reannounce_source = source
        
        # 检测周期跳变
        if state.cycle_synced and time_left > state.cached_time_left + 30:
            self._log('info', f"[{state.name}] 🔄 新周期 #{state.cycle_index + 1}")
            state.new_cycle(now, current_uploaded, time_left)
        
        state.cached_time_left = time_left
        
        if not state.cycle_synced and time_left > 0:
            state.cycle_synced = True
            state.cached_time_left = time_left
        
        # 计算限速
        new_limit, reason = self._calculate_limit(state, current_uploaded, now, time_left)
        
        # 应用限速
        if new_limit != state.last_limit:
            try:
                self.qb_manager.set_upload_limit(instance_id, hash, new_limit)
                state.last_limit = new_limit
                state.last_limit_reason = reason
            except Exception as e:
                self._log('debug', f"设置限速失败: {e}")
        
        # 日志
        if now - state.last_log_time > LimitConfig.LOG_INTERVAL:
            self._log_status(state, current_uploaded, current_speed, time_left, new_limit, reason)
            state.last_log_time = now
    
    def _get_reannounce_time(self, client, hash: str, tracker: str, 
                            state: TorrentLimitState, now: float) -> Tuple[float, str]:
        """获取汇报剩余时间"""
        time_left = state.cached_time_left
        
        # 方法1：从站点网页获取
        if self.site_helper_manager and PT_HELPER_AVAILABLE:
            try:
                helper = self.site_helper_manager.get_helper_by_tracker(tracker)
                if helper and helper.enabled:
                    if state.tid is None:
                        info = helper.search_tid_by_hash(hash)
                        if info and info.tid:
                            state.tid = info.tid
                            state.site_id = info.site_id
                    
                    if state.tid:
                        reannounce = helper.get_reannounce_time(tid=state.tid)
                        if reannounce is not None and reannounce > 0:
                            self._stats['site_success'] += 1
                            return float(reannounce), "site"
            except Exception as e:
                self._log('debug', f"站点获取汇报时间失败: {e}")
        
        # 方法2：从qB API获取
        try:
            props = client.torrents_properties(torrent_hash=hash)
            reannounce = props.get('reannounce', 0) or 0
            if 0 < reannounce < 86400:
                state.reannounce_time = now + reannounce
                self._stats['qb_api_success'] += 1
                return float(reannounce), "qb_api"
        except Exception as e:
            self._log('debug', f"qB API获取汇报时间失败: {e}")
        
        # 方法3：估算
        if state.reannounce_time > 0:
            estimated = max(0, state.reannounce_time - now)
            self._stats['fallback_count'] += 1
            return estimated, "estimated"
        
        self._stats['fallback_count'] += 1
        return time_left, "cached"
    
    def _calculate_limit(self, state: TorrentLimitState, current_uploaded: int, 
                         now: float, time_left: float) -> Tuple[int, str]:
        """计算限速值"""
        phase = state.get_phase(now)
        state.pid.set_phase(phase)
        
        elapsed = now - state.cycle_start
        total_cycle_time = elapsed + time_left
        target_total = state.target_speed * total_cycle_time
        cycle_uploaded = state.get_cycle_uploaded(current_uploaded)
        need_upload = max(0, target_total - cycle_uploaded)
        progress = safe_div(cycle_uploaded, target_total, 0)
        
        if time_left <= 0:
            return -1, "汇报中"
        
        required_speed = need_upload / time_left
        pid_output = state.pid.update(target_total, cycle_uploaded, now)
        params = LimitConfig.PID_PARAMS.get(phase, LimitConfig.PID_PARAMS['catch'])
        headroom = params.get('headroom', 1.02)
        
        src_tag = {"site": "🌐", "qb_api": "📡", "estimated": "⏱", "cached": "💾"}.get(
            state.reannounce_source, "❓"
        )
        
        if phase == 'finish':
            predicted_ratio = safe_div(cycle_uploaded + state.kalman.predict_upload(time_left), target_total, 0)
            if predicted_ratio > 1.002:
                correction = max(0.8, 1 - (predicted_ratio - 1) * 3)
            elif predicted_ratio < 0.998:
                correction = min(1.2, 1 + (1 - predicted_ratio) * 3)
            else:
                correction = 1.0
            limit = int(required_speed * pid_output * correction)
            reason = f"F:{int(required_speed/1024)}K{src_tag}"
            
        elif phase == 'steady':
            limit = int(required_speed * headroom * pid_output)
            reason = f"S:{int(required_speed/1024)}K{src_tag}"
            
        elif phase == 'catch':
            if required_speed > state.target_speed * 5:
                limit = -1
                reason = f"C:欠速{src_tag}"
            else:
                limit = int(required_speed * headroom * pid_output)
                reason = f"C:{int(required_speed/1024)}K{src_tag}"
                
        else:
            if progress >= 1.0:
                limit = LimitConfig.MIN_LIMIT
                reason = f"W:超{int((progress-1)*100)}%{src_tag}"
            elif progress >= 0.8:
                limit = int(required_speed * 1.01 * pid_output)
                reason = f"W:精控{src_tag}"
            elif progress >= 0.5:
                limit = int(required_speed * 1.05)
                reason = f"W:温控{src_tag}"
            else:
                limit = -1
                reason = f"W:预热{src_tag}"
        
        if limit > 0:
            limit = max(LimitConfig.MIN_LIMIT, min(LimitConfig.MAX_LIMIT, limit))
            step = 1024 if phase == 'finish' else 4096
            limit = int((limit + step // 2) // step) * step
        
        return limit, reason
    
    def _log_status(self, state: TorrentLimitState, uploaded: int, speed: float,
                    time_left: float, limit: int, reason: str):
        """记录状态日志"""
        phase = state.get_phase(time.time())
        cycle_uploaded = state.get_cycle_uploaded(uploaded)
        
        elapsed = time.time() - state.cycle_start
        total_time = elapsed + time_left
        target_total = state.target_speed * total_time
        progress = safe_div(cycle_uploaded, target_total, 0) * 100
        
        limit_str = 'MAX' if limit == -1 else f'{limit//1024}K'
        phase_emoji = {'warmup': '🔥', 'catch': '🏃', 'steady': '⚖️', 'finish': '🎯'}.get(phase, '❓')
        
        self._log('info', 
            f"[{state.name[:12]}] {phase_emoji} ↑{fmt_speed(speed)} "
            f"({progress:.0f}%) ⏱{time_left:.0f}s → {limit_str} ({reason})")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            'states_count': len(self._states),
            'running': self._running,
            **self._stats
        }
    
    def get_state(self, hash: str) -> Optional[Dict[str, Any]]:
        """获取单个种子的状态"""
        state = self._states.get(hash)
        if not state:
            return None
        
        now = time.time()
        cycle_time_left = max(0, state.reannounce_time - now) if state.reannounce_time > 0 else state.cached_time_left
        cycle_duration = now - state.cycle_start if state.cycle_start > 0 else 0
        
        # 获取当前种子的上传信息
        current_uploaded = 0
        current_speed = 0
        try:
            for inst_id, client in self.qb_manager._clients.items():
                if client:
                    torrents = client.torrents_info(hashes=hash)
                    if torrents:
                        t = torrents[0]
                        current_uploaded = t.uploaded
                        current_speed = t.upspeed
                        break
        except:
            pass
        
        # 计算周期内上传量
        cycle_uploaded = state.get_cycle_uploaded(current_uploaded)
        
        # 计算周期内平均速度
        cycle_avg_speed = cycle_uploaded / cycle_duration if cycle_duration > 0 else 0
        
        # 计算目标距离
        total_cycle_time = state.cached_time_left
        target_upload = state.target_speed * total_cycle_time if total_cycle_time > 0 else 0
        target_distance = target_upload - cycle_uploaded
        target_progress = (cycle_uploaded / target_upload * 100) if target_upload > 0 else 0
        
        return {
            'hash': state.hash,
            'name': state.name,
            'tracker': state.tracker,
            'instance_id': state.instance_id,
            'phase': state.get_phase(now),
            'cycle_index': state.cycle_index,
            'cycle_synced': state.cycle_synced,
            'time_left': cycle_time_left,
            'cycle_duration': cycle_duration,
            'total_cycle_time': total_cycle_time,
            'reannounce_source': state.reannounce_source,
            'target_speed': state.target_speed,
            'last_limit': state.last_limit,
            'last_limit_reason': state.last_limit_reason,
            'current_speed': current_speed,
            'cycle_uploaded': cycle_uploaded,
            'cycle_avg_speed': cycle_avg_speed,
            'target_upload': target_upload,
            'target_distance': target_distance,
            'target_progress': target_progress,
            'site_id': state.site_id,
            'tid': state.tid,
            'kalman_speed': state.kalman.speed,
            'kalman_predicted': state.kalman.predict_upload(cycle_time_left) if cycle_time_left > 0 else 0,
        }
    
    def get_all_states(self) -> List[Dict[str, Any]]:
        """获取所有种子状态"""
        states = []
        for h in self._states.keys():
            s = self.get_state(h)
            if s:
                states.append(s)
        return states


# ════════════════════════════════════════════════════════════════════════════════
# 工厂函数
# ════════════════════════════════════════════════════════════════════════════════
def create_precision_limit_engine(db, qb_manager, site_helper_manager=None, notifier=None):
    """创建精准限速引擎"""
    logger = logging.getLogger("limit_engine")
    
    if site_helper_manager is None and PT_HELPER_AVAILABLE:
        site_helper_manager = create_helper_manager()
    
    return PrecisionLimitEngine(db, qb_manager, site_helper_manager, notifier, logger)
